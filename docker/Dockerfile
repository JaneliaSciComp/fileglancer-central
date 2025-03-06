FROM ghcr.io/prefix-dev/pixi:0.42.1 AS build
ARG GIT_TAG=main

WORKDIR /app
RUN git clone --branch $GIT_TAG --depth 1 https://github.com/JaneliaSciComp/fileglancer-central.git . 

# install dependencies to `/app/.pixi/envs/prod`
# use `--locked` to ensure the lockfile is up to date with pixi.toml
RUN pixi install --locked -e prod \
    # create the shell-hook bash script to activate the environment
    && pixi shell-hook -e prod -s bash > /shell-hook \
    && echo "#!/bin/bash" > /app/entrypoint.sh \
    && cat /shell-hook >> /app/entrypoint.sh \
    # extend the shell-hook script to run the command passed to the container
    && echo 'exec "$@"' >> /app/entrypoint.sh

FROM ubuntu:24.04 AS production
WORKDIR /app
# copy the production environment into prod container
COPY --from=build /app/.pixi/envs/prod /app/.pixi/envs/prod
COPY --from=build --chmod=0755 /app/entrypoint.sh /app/entrypoint.sh

EXPOSE 7878
ENTRYPOINT [ "/app/entrypoint.sh" ]
CMD [ "uvicorn", "fileglancer_central:app", "--access-log", "--workers", "1", "--host", "0.0.0.0", "--port", "7878", "--forwarded-allow-ips='*'", "--proxy-headers" ]
