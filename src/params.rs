use std::io::Read;
use std::path::Path;
use std::{collections::HashMap, io::Write};

use chrono::{Datelike, Timelike};
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb_gridfs::options::GridFSUploadOptions;
use mongodb_gridfs::GridFSBucket;
use mongodb_gridfs_ext::bucket::common::GridFSBucketExt;
use mongodb_gridfs_ext::bucket::file_sync::FileSync;
use mongodb_gridfs_ext::error::Result as GridFSExtResult;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;
use zip::{self, write::FileOptions};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Param {
    StrParam {
        value: String,
    },
    EnvParam {
        name: String,
    },
    RemoteEnvParam {
        name: String,
    },
    CmdNameParam {
        name: String,
    },
    CmdPathParam {
        path: String,
    },
    InLocalFileParam {
        filepath: String,
        hostname: String,
    },
    OutLocalFileParam {
        filepath: String,
        hostname: String,
    },
    InCloudFileParam {
        filepath: String,
        hostname: String,
    },
    OutCloudFileParam {
        filepath: String,
        hostname: String,
    },
    FormatParam {
        tmpl: String,
        args: HashMap<String, Param>,
    },
}

impl Param {
    pub fn str<S: AsRef<str>>(value: S) -> Param {
        Param::StrParam {
            value: value.as_ref().to_string(),
        }
    }

    pub fn ipath<S: AsRef<str>>(filepath: S) -> Param {
        let filepath = filepath.as_ref().to_string();
        let hostname = hostname::get().unwrap().into_string().unwrap();
        Param::InLocalFileParam { filepath, hostname }
    }

    pub fn opath<S: AsRef<str>>(filepath: S) -> Param {
        let filepath = filepath.as_ref().to_string();
        let hostname = hostname::get().unwrap().into_string().unwrap();
        Param::OutLocalFileParam { filepath, hostname }
    }

    pub fn env<S: AsRef<str>>(name: S) -> Param {
        Param::EnvParam {
            name: name.as_ref().to_string(),
        }
    }

    pub fn remote_env<S: AsRef<str>>(name: S) -> Param {
        Param::RemoteEnvParam {
            name: name.as_ref().to_string(),
        }
    }

    pub fn cmd_name<S: AsRef<str>>(name: S) -> Param {
        Param::CmdNameParam {
            name: name.as_ref().to_string(),
        }
    }

    pub fn cmd_path<S: AsRef<str>>(path: S) -> Param {
        Param::CmdPathParam {
            path: path.as_ref().to_string(),
        }
    }

    pub fn format<S: AsRef<str>>(tmpl: S, args: HashMap<&str, Param>) -> Param {
        Param::FormatParam {
            tmpl: tmpl.as_ref().to_string(),
            args: args
                .into_iter()
                .map(|(key, param)| (key.to_string(), param))
                .collect(),
        }
    }

    pub fn hostname(&self) -> &str {
        match self {
            Param::InLocalFileParam { hostname, .. } => hostname,
            Param::OutLocalFileParam { hostname, .. } => hostname,
            Param::InCloudFileParam { hostname, .. } => hostname,
            Param::OutCloudFileParam { hostname, .. } => hostname,
            _ => unreachable!(),
        }
    }

    pub fn filepath(&self) -> &str {
        match self {
            Param::InLocalFileParam { filepath, .. } => filepath,
            Param::OutLocalFileParam { filepath, .. } => filepath,
            Param::InCloudFileParam { filepath, .. } => filepath,
            Param::OutCloudFileParam { filepath, .. } => filepath,
            _ => unreachable!(),
        }
    }

    pub fn is_input(&self) -> bool {
        matches!(
            self,
            Param::InLocalFileParam { .. } | Param::InCloudFileParam { .. }
        )
    }

    pub fn is_output(&self) -> bool {
        matches!(
            self,
            Param::OutLocalFileParam { .. } | Param::OutCloudFileParam { .. }
        )
    }

    pub fn is_local(&self) -> bool {
        matches!(
            self,
            Param::InLocalFileParam { .. } | Param::OutLocalFileParam { .. }
        )
    }

    pub fn is_cloud(&self) -> bool {
        matches!(
            self,
            Param::InCloudFileParam { .. } | Param::OutCloudFileParam { .. }
        )
    }

    pub fn as_cloud(&self) -> Param {
        match self.clone() {
            Param::InLocalFileParam { filepath, hostname } => {
                Param::InCloudFileParam { filepath, hostname }
            }
            Param::OutLocalFileParam { filepath, hostname } => {
                Param::OutCloudFileParam { filepath, hostname }
            }
            cloud @ Param::InCloudFileParam { .. } => cloud,
            cloud @ Param::OutCloudFileParam { .. } => cloud,
            _ => unreachable!(),
        }
    }

    pub fn cloud_url(&self) -> String {
        format!(
            "@{hostname}:{filepath}",
            hostname = self.hostname(),
            filepath = self.filepath()
        )
    }

    pub async fn id_on_cloud(&self, bucket: GridFSBucket) -> GridFSExtResult<ObjectId> {
        bucket.id(self.cloud_url().as_str()).await
    }

    pub async fn exists_on_cloud(&self, bucket: GridFSBucket) -> GridFSExtResult<bool> {
        bucket.exists(self.cloud_url().as_str()).await
    }

    pub async fn remove_from_cloud(&self, bucket: GridFSBucket) -> GridFSExtResult<()> {
        bucket
            .delete(self.id_on_cloud(bucket.clone()).await?)
            .await
            .map_err(Into::into)
    }

    pub async fn download(
        &self,
        bucket: GridFSBucket,
        filepath: impl AsRef<Path> + Send + Sync,
    ) -> GridFSExtResult<ObjectId> {
        let path = filepath.as_ref();
        let tmp_file = tempfile::Builder::new()
            .prefix(path.file_name().unwrap())
            .suffix(".parts")
            .tempfile_in(path.parent().unwrap())?;
        let oid = bucket
            .download_to(self.cloud_url().as_str(), tmp_file.path())
            .await?;

        if let Some(metadata) = bucket.metadata(oid).await? {
            if let Ok("application/directory+zip") = metadata.get_str("content_type") {
                unzip_all(tmp_file, path).unwrap();
                return Ok(oid);
            }
        }

        let (_, tmp_path) = tmp_file.keep().unwrap();
        std::fs::rename(tmp_path, path)?;
        Ok(oid)
    }

    pub async fn upload(
        &self,
        mut bucket: GridFSBucket,
        filepath: impl AsRef<Path> + Send,
    ) -> GridFSExtResult<ObjectId> {
        let filepath = filepath.as_ref();
        if filepath.is_dir() {
            let options = GridFSUploadOptions::builder()
                .metadata(Some(doc! {"content_type": "application/directory+zip"}))
                .build();
            let zip_file = tempfile::NamedTempFile::new()?;
            zip_dir(filepath, zip_file.path()).unwrap();

            return bucket
                .upload_from(self.cloud_url().as_str(), zip_file.path(), Some(options))
                .await;
        }

        bucket
            .upload_from(self.cloud_url().as_str(), filepath, None)
            .await
    }

    pub async fn download_inplace(&self, bucket: GridFSBucket) -> GridFSExtResult<ObjectId> {
        assert!(self.is_local());
        self.download(bucket, self.filepath()).await
    }

    pub async fn upload_inplace(&self, bucket: GridFSBucket) -> GridFSExtResult<ObjectId> {
        assert!(self.is_local());
        self.upload(bucket, self.filepath()).await
    }

    pub async fn download_to_string(&self, bucket: GridFSBucket) -> GridFSExtResult<String> {
        bucket.read_string(self.cloud_url().as_str()).await
    }

    pub async fn upload_from_string<S: AsRef<str>>(
        &self,
        mut bucket: GridFSBucket,
        content: S,
    ) -> GridFSExtResult<()> {
        bucket
            .write_string(self.cloud_url().as_str(), content.as_ref())
            .await
    }
}

fn unzip_all<R, P>(src: R, dst: P) -> zip::result::ZipResult<()>
where
    R: Read + std::io::Seek,
    P: AsRef<Path>,
{
    let dst = dst.as_ref();
    let mut archive = zip::ZipArchive::new(src)?;
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let out_path = match file.enclosed_name() {
            Some(path) => dst.join(path),
            None => continue,
        };

        if file.name().ends_with('/') {
            std::fs::create_dir_all(out_path).unwrap();
        } else {
            if let Some(outdir) = out_path.parent() {
                if !outdir.exists() {
                    std::fs::create_dir_all(outdir).unwrap()
                }
            }
            let mut outfile = std::fs::File::create(&out_path).unwrap();
            std::io::copy(&mut file, &mut outfile).unwrap();
        }

        // Get and set permissions
    }
    Ok(())
}

fn zip_dir<P: AsRef<Path>>(src: P, dst: P) -> zip::result::ZipResult<()> {
    let dst = std::fs::File::create(dst.as_ref()).unwrap();
    let mut zip = zip::ZipWriter::new(dst);
    for entry in WalkDir::new(src.as_ref()) {
        let entry = entry.unwrap();
        let path = entry.path();
        let metadata = path.metadata().unwrap();
        let mtime: chrono::DateTime<chrono::Local> =
            chrono::DateTime::from(metadata.modified().unwrap());
        let mtime = zip::DateTime::from_date_and_time(
            mtime.year() as u16,
            mtime.month() as u8,
            mtime.day() as u8,
            mtime.hour() as u8,
            mtime.minute() as u8,
            mtime.second() as u8,
        )
        .unwrap();
        let options = FileOptions::default().last_modified_time(mtime);
        let name = path.strip_prefix(src.as_ref()).unwrap().to_str().unwrap();
        if path.is_file() {
            zip.start_file(name, options)?;
            let buffer = std::fs::read(path).unwrap();
            zip.write_all(buffer.as_slice())?;
        } else if path.is_dir() {
            if path == src.as_ref() {
                continue;
            }
            zip.add_directory(name, options)?;
        }
    }
    zip.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod test_file_param {
        use std::io::Write;

        use chain_ext::mongodb_gridfs::DatabaseExt;
        use chain_ext::path::file_ext::FileExt;
        use fake::Fake;
        use test_utilities::docker;

        use super::*;

        #[test]
        fn test_conversion() {
            let fake_file = tempfile::NamedTempFile::new().unwrap();

            let param = Param::ipath(fake_file.path().to_str().unwrap());
            assert!(matches!(param, Param::InLocalFileParam { .. }));

            let param = param.as_cloud();
            assert!(matches!(param, Param::InCloudFileParam { .. }));

            let param = Param::opath(fake_file.path().to_str().unwrap());
            assert!(matches!(param, Param::OutLocalFileParam { .. }));

            let param = param.as_cloud();
            assert!(matches!(param, Param::OutCloudFileParam { .. }));
        }

        #[tokio::test]
        async fn test_upload_download() {
            let workspace = tempfile::tempdir().unwrap();

            let container = docker::Builder::new("mongo")
                .name("cmdproxy-test-params")
                .bind_port_as_default(Some("0"), "27017")
                .build_disposable()
                .await;

            let bucket = mongodb::Client::with_uri_str(container.url())
                .await
                .unwrap()
                .database("cmdproxy-test-params-db")
                .bucket(None);

            let mut fake_file = tempfile::NamedTempFile::new_in(workspace.path()).unwrap();
            let fake_filepath = fake_file.path().to_str().unwrap().to_owned();
            let fake_content = (20..40).fake::<String>();
            fake_file.write_all(fake_content.as_bytes()).unwrap();

            let param = Param::ipath(fake_filepath.as_str());
            let uploaded_id = param
                .upload(bucket.clone(), fake_filepath.as_str())
                .await
                .unwrap();

            let content_on_cloud = bucket
                .clone()
                .read_string(param.cloud_url().as_str())
                .await
                .unwrap();

            // assert upload
            assert_eq!(
                content_on_cloud,
                std::fs::read_to_string(fake_filepath.as_str()).unwrap()
            );

            let downloaded_file = tempfile::NamedTempFile::new_in(workspace.path()).unwrap();
            let downloaded_filepath = downloaded_file.path();
            let downloaded_id = param
                .download(bucket.clone(), downloaded_filepath)
                .await
                .unwrap();

            // assert download
            assert_eq!(uploaded_id, downloaded_id);
            assert_eq!(
                std::fs::read_to_string(downloaded_filepath).unwrap(),
                std::fs::read_to_string(fake_filepath.as_str()).unwrap()
            );
        }

        #[test]
        fn test_zip_dir() {
            let workspace = tempfile::tempdir().unwrap();
            let tmp_zip_file = tempfile::NamedTempFile::new_in(workspace.path()).unwrap();
            let tmp_zip_path = tmp_zip_file.path();

            let project_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let resources_dir = project_root.join("resources/test");
            let expected_zip_path = resources_dir.join("fake_folder.zip");
            let fake_folder_path = resources_dir.join("fake_folder");

            zip_dir(fake_folder_path.as_path(), tmp_zip_path).unwrap();

            assert_eq!(
                std::fs::read(tmp_zip_path).unwrap(),
                std::fs::read(expected_zip_path.as_path()).unwrap()
            )
        }

        #[test]
        fn test_unzip_all() {
            let workspace = tempfile::tempdir().unwrap();

            let project_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let resources_dir = project_root.join("resources/test");
            let expected_zip_path = resources_dir.join("fake_folder.zip");
            let fake_folder_path = resources_dir.join("fake_folder");

            let zip_file = expected_zip_path.open().unwrap();
            unzip_all(zip_file, workspace.path()).unwrap();

            let res = folder_compare::FolderCompare::new(
                fake_folder_path.as_path(),
                workspace.path(),
                &vec![],
            )
            .unwrap();
            assert!(res.changed_files.is_empty());
            assert!(res.new_files.is_empty());
        }
    }
}
