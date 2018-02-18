# Changelog - amazonka-s3-streaming

## 0.2.0.5
- Fix compatibility with 

## 0.2.0.4
- Make building s3upload executable optional

## 0.2.0.3
 * Make all library generated messages use Debug level not Info

## 0.2.0.2
 * Update to mmorph < 1.2

## 0.2.0.1
 * Fixed a bug with the printf format strings which would lead to a crash (Thanks @JakeOShannessy
   for reporting).

## 0.2.0.0
 * Fixed a potential bug with very large uploads where the chunksize might be too small
   for the limit of 10,000 chunks per upload (#6).
 * Change API to allow the user to specify a chunk size for streaming if the user knows
   more about the data than we do.
 * Allow the user to specify how many concurrent threads to use for `concurrentUpload` as
   as well as chunk size (#4).
 * Better specify cabal dependency ranges.