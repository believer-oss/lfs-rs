# Third-party patches

These are patches we incorporated from upstream forks. Since we're in a private fork unconnected to the original, merging these requires pulling them in as patches.

## Make encryption optional

optional-encryption.patch
[js6pak/jasonwhite patch](https://github.com/jasonwhite/rudolfs/commit/ce4541ef6900174e4e9ed4a41bb24739aab10e25)

This patch passes files without applying encryption. The bucket is already storing the items with a default server-side encryption policy, using SSE-S3 managed keys. This seems sufficient, without applying additional encryption overhead on top.

The original patch was written by the upstream maintainer, jason white, but updated by js6pak after it was not merged.

## Add very simple github auth

github-token-authentication.patch
[js6pak](https://github.com/jasonwhite/rudolfs/commit/3ed2b40967474b9ca14ab6e942259cc8d8c4a443)

This patch tests the authorization header against the github repo api to ensure the provided token has pull/push authorization to access the repo. This is a bit suboptimal, since it's only validating the batch api call and not the individual object calls, but that is exactly how it would work with pre-signed S3 urls as well. Depending on the value local caching provides, we may want to switch to using direct S3 calls in the future regardless.

## Authorization header passing (unapplied)

pass-authorization-headers.patch
[Steven-Chan](https://github.com/jasonwhite/rudolfs/commit/dbe4fcfcfc82db7ca7ba903f9b14378cfaf051da)

Keeping this one around for posterity, but we likely won't need this. If we do end up needing to copy the inbound batch api request authorization headers to the object downloads, this patch could be helpful.
