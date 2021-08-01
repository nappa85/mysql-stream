#![deny(warnings)]
#![deny(missing_docs)]

//! # mysql-stream
//!
//! futures-code::Stream adapter for mysql_async

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

/// Facility to have a single error type
#[derive(Debug)]
pub enum StreamError {
    /// Row conversion error (e.g. type conversion error)
    Row(FromRowError),
    /// General error (e.g. network error)
    General(mysql_async::Error),
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::Row(r) => r.fmt(f),
            StreamError::General(g) => g.fmt(f),
        }
    }
}

impl std::error::Error for StreamError {}

impl From<FromRowError> for StreamError {
    fn from(e: FromRowError) -> Self {
        StreamError::Row(e)
    }
}

impl From<mysql_async::Error> for StreamError {
    fn from(e: mysql_async::Error) -> Self {
        StreamError::General(e)
    }
}

/// Transforms a mysql_async::QueryResult into a futures-core::Stream, mutable reference version
pub fn stream<'q, 'a: 'q, 't: 'a, P: Protocol + Unpin, T: FromRow + Unpin>(
    inner: &'q mut QueryResult<'a, 't, P>,
) -> Stream<'q, 'a, 't, P, T> {
    let state = StreamState::QueryResult(CowMut::Borrowed(inner));
    Stream {
        state,
        _marker: PhantomData,
    }
}

/// Transforms a mysql_async::QueryResult into a futures-core::Stream, owned version
pub fn stream_and_drop<'q, 'a: 'q, 't: 'a, P: Protocol + Unpin, T: FromRow + Unpin>(
    inner: QueryResult<'a, 't, P>,
) -> Stream<'q, 'a, 't, P, T> {
    let state = StreamState::QueryResult(CowMut::Owned(inner));
    Stream {
        state,
        _marker: PhantomData,
    }
}

/// Intermediate type to implement futures-core::Stream for mysql_async::QueryResult
pub struct Stream<'q, 'a, 't, P, T> {
    state: StreamState<'q, 'a, 't, P, T>,
    _marker: PhantomData<T>,
}

enum StreamState<'q, 'a, 't, P, T> {
    QueryResult(CowMut<'q, 'a, 't, P>),
    NextFut(BoxFuture<'q, (NextResult, CowMut<'q, 'a, 't, P>)>),
    DropFut(BoxFuture<'q, Result<Option<T>, StreamError>>),
    Done,
    Invalid,
}

impl<'q, 'a, 't, P, T> StreamState<'q, 'a, 't, P, T> {
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
    type Item = Result<T, StreamError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let state = &mut self.get_mut().state;
        match state.take() {
            StreamState::QueryResult(mut query_result) => {
                let fut = Box::pin(async move {
                    let row = query_result.as_mut().next().await;
                    (row, query_result)
                });

                handle_next(fut, state, cx)
            }
            StreamState::NextFut(fut) => handle_next(fut, state, cx),
            StreamState::DropFut(fut) => handle_drop(fut, state, cx),
            StreamState::Done => Poll::Ready(None),
            StreamState::Invalid => unreachable!(),
        }
    }
}

fn handle_next<'q, 'a: 'q, 't: 'a, P: Protocol, T>(
    mut fut: BoxFuture<'q, (NextResult, CowMut<'q, 'a, 't, P>)>,
    state: &mut StreamState<'q, 'a, 't, P, T>,
    cx: &mut Context,
) -> Poll<Option<Result<T, StreamError>>>
where
    T: FromRow,
{
    let (next_result, query_result) = match fut.as_mut().poll(cx) {
        Poll::Ready(ready) => ready,
        Poll::Pending => {
            *state = StreamState::NextFut(fut);
            return Poll::Pending;
        }
    };
    Poll::Ready(match next_result {
        Ok(None) => {
            if let CowMut::Owned(qr) = query_result {
                *state = StreamState::DropFut(Box::pin(async move {
                    #[cfg(test)]
                    eprintln!("Dropping on end");

                    qr.drop_result().await?;
                    Ok(None)
                }));
                return Poll::Pending;
            }

            *state = StreamState::Done;
            None
        },
        res => {
            let res = res.map_err(StreamError::from)
                .and_then(|row| from_row_opt::<T>(row.unwrap()).map_err(StreamError::from));

            // TryCollect workaround
            // Why this is needed? Because TryCollect stops at first error encountered, so we'll never reach None and never drop_result
            if res.is_err() {
                if let CowMut::Owned(qr) = query_result {
                    if let Err(e) = res {
                        *state = StreamState::DropFut(Box::pin(async move {
                            #[cfg(test)]
                            eprintln!("Dropping on error");

                            qr.drop_result().await?;
                            Err(e)
                        }));
                        return Poll::Pending;
                    }
                    else {
                        unreachable!();
                    }
                }
            }

            *state = StreamState::QueryResult(query_result);
            Some(res)
        }
    })
}

fn handle_drop<'q, 'a: 'q, 't: 'a, P: Protocol, T>(
    mut fut: BoxFuture<'q, Result<Option<T>, StreamError>>,
    state: &mut StreamState<'q, 'a, 't, P, T>,
    cx: &mut Context,
) -> Poll<Option<Result<T, StreamError>>>
where
    T: FromRow,
{
    match fut.as_mut().poll(cx) {
        Poll::Ready(r) => Poll::Ready(r.transpose()),
        Poll::Pending => {
            *state = StreamState::DropFut(fut);
            Poll::Pending
        }
    }
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
        qr.drop_result().await.expect("Drop failed");
        assert!(res.len() == 1);
        println!("{}", res[0].timestamp);
    }

    #[tokio::test]
    async fn stream_err() {
        let url = env::var("DATABASE_URL").expect("Missing DATABASE_URL env var");
        let mut conn = Conn::from_url(url)
            .await
            .expect("Connection failed");
        let mut qr = conn.query_iter("SELECT 'abc'")
            .await
            .expect("Query failed");
        let res = super::stream::<_, Foo>(&mut qr).try_collect::<Vec<_>>().await;
        qr.drop_result().await.expect("Drop failed");
        assert!(res.is_err());
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

    #[tokio::test]
    async fn stream_and_drop_err() {
        let url = env::var("DATABASE_URL").expect("Missing DATABASE_URL env var");
        let mut conn = Conn::from_url(url)
            .await
            .expect("Connection failed");
        let qr = conn.query_iter("SELECT 'abc'")
            .await
            .expect("Query failed");
        let res = super::stream_and_drop::<_, Foo>(qr).try_collect::<Vec<_>>().await;
        assert!(res.is_err());
    }
}