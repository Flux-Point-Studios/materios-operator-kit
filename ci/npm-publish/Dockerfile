FROM node:24-alpine@sha256:ebfe2f90462722a7a4de65e91990e97fe0d401c70e0e762c5b53302f905ec1c1
# Statically linked (static-pie), so LD_PRELOAD in the step's environment loads nothing into it.
COPY --from=busybox:1.37.0-musl@sha256:5cec3fc171c87218698e85a52af7087de727372aae264a787b8112901a5b0092 /bin/busybox /static/busybox
COPY --chmod=644 npm-publish.sh /npm-publish.sh
ENTRYPOINT ["/static/busybox", "sh", "/npm-publish.sh"]
