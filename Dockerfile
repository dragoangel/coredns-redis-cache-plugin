# syntax=docker/dockerfile:1
#
# Build of stock CoreDNS + the redis_cache plugin from this repository.
#
# Unlike a "fetch from the Go proxy" build, this image compiles the plugin from
# the local checkout (the build context) via a `go mod replace`, so the image
# always reflects the exact commit/tag being built — correct on PRs, branches
# and freshly-pushed tags that the proxy hasn't indexed yet.
#
#   docker build -t coredns-redis .
#   # or, with overrides:
#   docker build \
#       --build-arg COREDNS_VERSION=v1.14.3 \
#       -t coredns-redis .
#
# How it works: CoreDNS builds its plugin chain from `plugin.cfg` via `go generate`
# (which writes core/plugin/zplugin.go + core/dnsserver/zdirectives.go). We clone
# CoreDNS, splice the redis_cache line into plugin.cfg right after `cache:cache`
# (so cache=L1, redis_cache=L2), point the plugin module at the local source with
# a replace directive, then `go mod tidy` — which pulls the plugin's own
# dependencies (go-redis, etc.) from the local go.mod. No manual deps.

ARG GO_VERSION=1.26

########################################
# Build stage
########################################
FROM golang:${GO_VERSION} AS build

# Empty COREDNS_VERSION => auto-detect the latest vX.Y.Z release tag.
ARG COREDNS_VERSION=
ARG PLUGIN_PATH=github.com/dragoangel/coredns-redis-cache-plugin
# Directive line injected into plugin.cfg, placed immediately AFTER this anchor.
ARG PLUGIN_CFG_LINE=redis_cache:github.com/dragoangel/coredns-redis-cache-plugin
ARG PLUGIN_CFG_AFTER=cache:cache

WORKDIR /build

# 0. Copy the plugin source (this repo) so we can build it from the local checkout.
COPY . /src

# 1. Resolve version + clone CoreDNS at that tag.
RUN set -eux; \
    v="${COREDNS_VERSION}"; \
    if [ -z "$v" ]; then \
      v="$(git ls-remote --tags --refs --sort='-v:refname' \
            https://github.com/coredns/coredns.git 'refs/tags/v[0-9]*.[0-9]*.[0-9]*' \
          | head -1 | sed 's|.*refs/tags/||')"; \
    fi; \
    test -n "$v"; \
    echo "Building CoreDNS $v"; \
    git clone --depth 1 --branch "$v" https://github.com/coredns/coredns.git .

# 2. Inject the external plugin into plugin.cfg (keep all stock plugins; just add ours).
RUN set -eux; \
    grep -qx "${PLUGIN_CFG_AFTER}" plugin.cfg || { echo "anchor '${PLUGIN_CFG_AFTER}' not found in plugin.cfg" >&2; exit 1; }; \
    grep -q "^${PLUGIN_CFG_LINE%%:*}:" plugin.cfg || \
      sed -i "\|^${PLUGIN_CFG_AFTER}\$|a ${PLUGIN_CFG_LINE}" plugin.cfg; \
    echo "----- plugin.cfg (cache/redis region) -----"; \
    grep -nE 'cache|redis' plugin.cfg

# 3. Wire the plugin from local source, regenerate the plugin chain, tidy, build.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    go mod edit -require="${PLUGIN_PATH}@v0.0.0"; \
    go mod edit -replace="${PLUGIN_PATH}=/src"; \
    go generate coredns.go; \
    go mod tidy; \
    echo "----- resolved redis-related requirements -----"; \
    go list -m all | grep -iE 'redis|dragoangel' || true; \
    CGO_ENABLED=0 GOFLAGS=-trimpath go build -ldflags="-s -w" -o /coredns .

########################################
# Runtime stage
########################################
FROM gcr.io/distroless/static-debian12
COPY --from=build /coredns /coredns
# NOTE: port 53 is privileged. The nonroot user needs CAP_NET_BIND_SERVICE
# (k8s securityContext.capabilities / `docker run --cap-add=NET_BIND_SERVICE`),
# or run as root.
USER 65532:65532
EXPOSE 53 53/udp 9153
ENTRYPOINT ["/coredns"]
CMD ["-conf", "/etc/coredns/Corefile"]
