use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use mysql_async::{
    from_row_opt,
    prelude::{FromRow, Protocol},
    FromRowError, QueryResult, Row,
};

#[derive(Debug)]
pub enum StreamErr {
    Row(FromRowError),
    General(mysql_async::Error),
}

impl std::fmt::Display for StreamErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamErr::Row(r) => r.fmt(f),
            StreamErr::General(g) => g.fmt(f),
        }
    }
}

impl std::error::Error for StreamErr {}

impl From<FromRowError> for StreamErr {
    fn from(e: FromRowError) -> Self {
        StreamErr::Row(e)
    }
}

impl From<mysql_async::Error> for StreamErr {
    fn from(e: mysql_async::Error) -> Self {
        StreamErr::General(e)
    }
}

pub fn stream<'q, 'a: 'q, 't: 'a, P: Protocol + Unpin, T: FromRow + Unpin>(
    inner: &'q mut QueryResult<'a, 't, P>,
) -> Stream<'q, 'a, 't, P, T> {
    let state = StreamState::QueryResult(CowMut::Borrowed(inner));
    Stream {
        state,
        _marker: PhantomData,
    }
}

pub fn stream_and_drop<'q, 'a: 'q, 't: 'a, P: Protocol + Unpin, T: FromRow + Unpin>(
    inner: QueryResult<'a, 't, P>,
) -> Stream<'q, 'a, 't, P, T> {
    let state = StreamState::QueryResult(CowMut::Owned(inner));
    Stream {
        state,
        _marker: PhantomData,
    }
}

pub struct Stream<'q, 'a, 't, P, T> {
    state: StreamState<
        CowMut<'q, 'a, 't, P>,
        BoxFuture<'q, (NextResult, CowMut<'q, 'a, 't, P>)>,
    >,
    _marker: PhantomData<T>,
}

enum StreamState<QR, F> {
    QueryResult(QR),
    Fut(F),
    Done,
    Invalid,
}

impl<QR, F> StreamState<QR, F> {
    fn take(&mut self) -> Self {
        match std::mem::replace(self, Self::Invalid) {
            Self::Invalid => panic!("Stream state is invalid"),
            state => state,
        }
    }
}

type NextResult = Result<Option<Row>, mysql_async::Error>;

enum CowMut<'q, 'a: 'q, 't: 'a, P> {
    Borrowed(&'q mut QueryResult<'a, 't, P>),
    Owned(QueryResult<'a, 't, P>),
}

impl<'q, 'a: 'q, 't: 'a, P> AsMut<QueryResult<'a, 't, P>> for CowMut<'q, 'a, 't, P> {
    fn as_mut(&mut self) -> &mut QueryResult<'a, 't, P> {
        match self {
            CowMut::Borrowed(q) => q,
            CowMut::Owned(q) => q,
        }
    }
}

impl<'q, 'a: 'q, 't: 'a, P: Protocol + Unpin, T: FromRow + Unpin> futures_util::Stream
    for Stream<'q, 'a, 't, P, T>
{
    type Item = Result<T, StreamErr>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let state = &mut self.get_mut().state;
        match state.take() {
            StreamState::QueryResult(mut query_result) => {
                let fut = Box::pin(async move {
                    let row = query_result.as_mut().next().await;
                    (row, query_result)
                });

                handle_future(fut, state, cx)
            }
            StreamState::Fut(fut) => handle_future(fut, state, cx),
            StreamState::Done => Poll::Ready(None),
            StreamState::Invalid => unreachable!(),
        }
    }
}

fn handle_future<'q, 'a: 'q, 't: 'a, P, T>(
    mut fut: BoxFuture<'q, (NextResult, CowMut<'q, 'a, 't, P>)>,
    state: &mut StreamState<
        CowMut<'q, 'a, 't, P>,
        BoxFuture<'q, (NextResult, CowMut<'q, 'a, 't, P>)>,
    >,
    cx: &mut Context,
) -> Poll<Option<Result<T, StreamErr>>>
where
    T: FromRow,
{
    let (next_result, query_result) = match fut.as_mut().poll(cx) {
        Poll::Ready(ready) => ready,
        Poll::Pending => {
            *state = StreamState::Fut(fut);
            return Poll::Pending;
        }
    };
    Poll::Ready(match next_result {
        Ok(Some(row)) => {
            *state = StreamState::QueryResult(query_result);
            Some(from_row_opt(row).map_err(StreamErr::from))
        }
        Ok(None) => {
            *state = StreamState::Done;
            None
        }
        Err(e) => {
            *state = StreamState::QueryResult(query_result);
            Some(Err(StreamErr::from(e)))
        }
    })
}

#[cfg(test)]
mod test {
    use std::env;
    use mysql_async::{Conn, Row, FromRowError, prelude::{FromRow, Queryable}};
    use futures_util::TryStreamExt;

    struct Foo {
        timestamp: i64,
    }

    impl FromRow for Foo {
        fn from_row_opt(mut row: Row) -> Result<Self, FromRowError> {
            if let Some(Ok(timestamp)) = row.take_opt(0) {
                Ok(Foo {
                    timestamp
                })
            }
            else {
                Err(FromRowError(row))
            }
        }
    }

    #[tokio::test]
    async fn stream() {
        let url = env::var("DATABASE_URL").expect("Missing DATABASE_URL env var");
        let mut conn = Conn::from_url(url)
            .await
            .expect("Connection failed");
        let mut qr = conn.query_iter("SELECT UNIX_TIMESTAMP()")
            .await
            .expect("Query failed");
        let res: Vec<Foo> = super::stream(&mut qr).try_collect().await.expect("Collect failed");
        assert!(res.len() == 1);
        println!("{}", res[0].timestamp);
    }

    #[tokio::test]
    async fn stream_and_drop() {
        let url = env::var("DATABASE_URL").expect("Missing DATABASE_URL env var");
        let mut conn = Conn::from_url(url)
            .await
            .expect("Connection failed");
        let qr = conn.query_iter("SELECT UNIX_TIMESTAMP()")
            .await
            .expect("Query failed");
        let res: Vec<Foo> = super::stream_and_drop(qr).try_collect().await.expect("Collect failed");
        assert!(res.len() == 1);
        println!("{}", res[0].timestamp);
    }
}