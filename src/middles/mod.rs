use celery::export::async_trait;

pub(crate) mod invoke;
pub(crate) mod serde;

#[async_trait]
pub(crate) trait Middle<Request, Response, IRequest, IResponse>
where
    Request: Send,
    Response: Send,
    IRequest: Send,
    IResponse: Send,
{
    async fn transform_request(&self, request: Request) -> anyhow::Result<IRequest>;

    async fn transform_response(
        &self,
        response: anyhow::Result<IResponse>,
    ) -> anyhow::Result<Response>;
}
