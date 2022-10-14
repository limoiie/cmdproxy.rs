import dataclasses
import json
from typing import Optional

import faker
import fire
from celery import Celery, Task
from faker.providers import file
from gridfs import GridFS, GridOut
from pymongo import MongoClient

_faker = faker.Faker()


@dataclasses.dataclass
class RunRequest:
    command: str
    args: list[str]
    to_downloads: Optional[list[(str, str)]]
    to_uploads: Optional[list[str, str]]
    stdout: Optional[str]
    stderr: Optional[str]


def main(redis_url: str = "redis://localhost:6379/",
         mongo_url: str = "mongodb://localhost:27017/",
         mongodb_name: str = "testdb"):
    print(f'redis run on: {redis_url}')
    print(f'mongo run on: {mongo_url}')
    print(f'mongo dbname: {mongodb_name}')

    gridfs = GridFS(MongoClient(mongo_url).get_database(mongodb_name))

    app = Celery('Bin2DataDaemon', broker=redis_url)

    @app.task(name='run')
    def run(run_request: str):
        print(f'Received args: {run_request}')

    input_link, input_content = __fake_cloud_file(gridfs)
    output_link = _faker.file_name()
    stdout_link, stdout_content = 'stdout' + _faker.file_name(), 'hello'

    print(f'filename of cloud input: {input_link}')
    print(f'filename of cloud output: {output_link}')

    req = RunRequest(command='sh', args=[
        '-c', f'echo {stdout_content} && '
              f'cat <#:i>{input_link}</> > <#:o>{output_link}</>'
    ], to_downloads=None, to_uploads=None, stdout=stdout_link, stderr=None)
    serialized_req = json.dumps(dataclasses.asdict(req), separators=(',', ':'))

    run: Task
    run.apply_async(args=(serialized_req,), queue='sh')

    input('press enter to continue')

    print('checking normal output')
    output_content = __read_cloud_file(gridfs, output_link)
    assert input_content == output_content

    print('checking stdout output')
    output_stdout_content = __read_cloud_file(gridfs, stdout_link)
    assert stdout_content + '\n' == output_stdout_content.decode('utf-8')

    print('bingo!')


def __fake_cloud_file(gridfs: GridFS):
    link = _faker.file_name()
    content = _faker.text().encode(encoding='utf-8')

    with gridfs.new_file(filename=link) as f:
        f.write(content)

    return link, content


def __read_cloud_file(gridfs: GridFS, link):
    out_file = gridfs.find_one(filter={'filename': link})
    assert isinstance(out_file, GridOut), f'Failed to read {link} from gridfs'

    with out_file as f:
        output_content = f.read()

    print(f'download from {link}: ', output_content)
    return output_content


if __name__ == '__main__':
    _faker.add_provider(file)
    fire.Fire(main)
