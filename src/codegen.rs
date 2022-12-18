#[doc(hidden)]
#[macro_export]
macro_rules! __apply_middles_in_stack {
    (
        $arg:expr,
        $func:expr,
        [ $middle:expr $(, $middles:expr )* ]
    ) => {{
        let middle = $middle;
        let arg = middle.transform_request($arg).await;
        match arg {
            Ok(arg) => {
                let arg = $crate::__apply_middles_in_stack!(arg, $func, [ $($middles),* ]);
                let arg = middle.transform_response(arg).await;
                arg
            },
            Err(err) => Err(err),
        }
    }};
    ( $arg:expr, $func:expr, [] ) => { $func($arg).await };
}

#[macro_export]
macro_rules! apply_middles {
    (
        $init:expr,
        $( >=< [$middles:expr] )+
        >>= $func:ident
    ) => {
        $crate::__apply_middles_in_stack!( $init, $func, [ $( $middles ),* ] )
    };
}
