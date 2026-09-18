#!/usr/bin/env bash
set -Eeuo pipefail
trap 'printf "部署失败（第 %s 行）。请检查上方错误和 docker compose logs。\n" "$LINENO" >&2' ERR

# Usage: PUBLIC_ORIGIN=https://drop.example.com bash scripts/deploy.sh
# Node and Maven run inside Docker; only Git, Docker and Compose v2 are needed.
DEPLOY_DIR="${DEPLOY_DIR:-$HOME/filedrop-deploy}"
HTTP_PORT="${HTTP_PORT:-8080}"
PUBLIC_ORIGIN="${PUBLIC_ORIGIN:-http://localhost:$HTTP_PORT}"
WEB_REPO="${WEB_REPO:-https://github.com/KokoHere02/FileDrop-web.git}"
API_REPO="${API_REPO:-https://github.com/KokoHere02/FileDrop-Java.git}"
WEB_BRANCH="${WEB_BRANCH:-}"
API_BRANCH="${API_BRANCH:-}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-filedrop}"
VITE_ICE_SERVERS="${VITE_ICE_SERVERS:-}"

die() { printf '%s\n' "$*" >&2; exit 1; }
for command in git docker; do
    command -v "$command" >/dev/null || die "请先安装 $command。"
done
docker compose version >/dev/null || die '请先安装 Docker Compose v2 插件。'
docker info >/dev/null || die 'Docker 未启动，或当前用户无权访问 Docker。'
[[ "$HTTP_PORT" =~ ^[0-9]{1,5}$ ]] && (( 10#$HTTP_PORT >= 1 && 10#$HTTP_PORT <= 65535 )) \
    || die 'HTTP_PORT 必须是 1–65535 之间的端口。'
HTTP_PORT="$((10#$HTTP_PORT))"
[[ "$PUBLIC_ORIGIN" =~ ^https?://[^/[:space:],]+$ ]] \
    || die 'PUBLIC_ORIGIN 必须是浏览器访问的来源，例如 https://drop.example.com（不要带路径或末尾斜杠）。'
[[ "$COMPOSE_PROJECT_NAME" =~ ^[a-z0-9][a-z0-9_-]*$ ]] \
    || die 'COMPOSE_PROJECT_NAME 只能包含小写字母、数字、下划线和短横线。'

mkdir -p "$DEPLOY_DIR"
DEPLOY_DIR="$(cd "$DEPLOY_DIR" && pwd)"
[[ "$DEPLOY_DIR" != / ]] || die 'DEPLOY_DIR 不可为根目录。'

sync_repo() {
    local url="$1" directory="$2" branch="$3"
    if [[ ! -e "$directory" ]]; then
        if [[ -n "$branch" ]]; then
            git clone --branch "$branch" --single-branch "$url" "$directory"
        else
            git clone "$url" "$directory"
        fi
    else
        [[ -d "$directory/.git" ]] || die "$directory 已存在但不是 Git 仓库。"
        [[ "$(git -C "$directory" remote get-url origin)" == "$url" ]] \
            || die "$directory 的 origin 与配置不符。"
        [[ -z "$(git -C "$directory" status --porcelain)" ]] \
            || die "$directory 有本地修改，请先处理后重试。"
        local current
        current="$(git -C "$directory" symbolic-ref --quiet --short HEAD)" \
            || die "$directory 当前不是分支检出状态。"
        branch="${branch:-$current}"
        [[ "$current" == "$branch" ]] || die "$directory 当前分支是 $current，与 $branch 不符。"
        git -C "$directory" fetch origin "$branch"
        git -C "$directory" merge --ff-only FETCH_HEAD
        [[ "$(git -C "$directory" rev-parse HEAD)" == "$(git -C "$directory" rev-parse FETCH_HEAD)" ]] \
            || die "$directory 包含本地提交，请先处理后重试。"
    fi
    printf '%s: %s\n' "$directory" "$(git -C "$directory" rev-parse HEAD)"
}

sync_repo "$WEB_REPO" "$DEPLOY_DIR/web" "$WEB_BRANCH"
sync_repo "$API_REPO" "$DEPLOY_DIR/api" "$API_BRANCH"
[[ -f "$DEPLOY_DIR/web/package-lock.json" ]] || die '前端缺少 package-lock.json，无法执行 npm ci。'
[[ -f "$DEPLOY_DIR/api/pom.xml" ]] || die '后端缺少 pom.xml。'

cat > "$DEPLOY_DIR/frontend.Dockerfile" <<'EOF'
FROM node:22-bookworm-slim AS build
WORKDIR /build
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ARG VITE_ICE_SERVERS=""
ENV VITE_API_BASE_URL=/api VITE_SIGNALING_URL=/api/ws
ENV VITE_ICE_SERVERS=${VITE_ICE_SERVERS}
RUN npm run build && test -f dist/index.html

FROM nginx:stable-alpine
COPY --from=build /build/dist /usr/share/nginx/html
EXPOSE 80
EOF

cat > "$DEPLOY_DIR/frontend.Dockerfile.dockerignore" <<'EOF'
.git
node_modules
dist
.env
.env.*
*.log
EOF

# Keep deployment independent of whether the backend has committed a Dockerfile.
cat > "$DEPLOY_DIR/backend.Dockerfile" <<'EOF'
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml ./
RUN mvn -B -ntp dependency:go-offline
COPY src ./src
RUN mvn -B -ntp package -DskipTests && cp target/*.jar /build/app.jar

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app
COPY --from=build --chown=app:app /build/app.jar ./app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
EOF

cat > "$DEPLOY_DIR/backend.Dockerfile.dockerignore" <<'EOF'
**
!pom.xml
!src/
!src/**
EOF

cat > "$DEPLOY_DIR/nginx.conf" <<'EOF'
map $http_upgrade $connection_upgrade {
    default upgrade;
    '' close;
}
server {
    listen 80;
    server_name _;
    root /usr/share/nginx/html;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location /api/ {
        # Docker DNS: resolve again after backend container replacement.
        resolver 127.0.0.11 valid=10s ipv6=off;
        set $backend http://backend:8080;
        proxy_pass $backend$request_uri;
        proxy_http_version 1.1;
        proxy_set_header Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
        proxy_buffering off;
    }
}
EOF

cat > "$DEPLOY_DIR/compose.yaml" <<'EOF'
services:
  backend:
    build:
      context: ./api
      dockerfile: ../backend.Dockerfile
    environment:
      FILEDROP_ALLOWED_ORIGINS: ${PUBLIC_ORIGIN:?Set PUBLIC_ORIGIN}
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8080"]
      interval: 10s
      timeout: 3s
      retries: 12
      start_period: 30s
    logging:
      driver: json-file
      options:
        max-size: 10m
        max-file: "3"

  frontend:
    build:
      context: ./web
      dockerfile: ../frontend.Dockerfile
      args:
        VITE_ICE_SERVERS: ${VITE_ICE_SERVERS:-}
    ports:
      - "${HTTP_PORT:-8080}:80"
    volumes:
      - ./nginx.conf:/etc/nginx/conf.d/default.conf:ro
    depends_on:
      backend:
        condition: service_healthy
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://127.0.0.1/"]
      interval: 10s
      timeout: 3s
      retries: 6
    logging:
      driver: json-file
      options:
        max-size: 10m
        max-file: "3"
EOF

# Persist Compose settings for subsequent logs/stop/start commands.
umask 077
printf 'PUBLIC_ORIGIN=%s\nHTTP_PORT=%s\nCOMPOSE_PROJECT_NAME=%s\n' \
    "$PUBLIC_ORIGIN" "$HTTP_PORT" "$COMPOSE_PROJECT_NAME" > "$DEPLOY_DIR/.env"
export PUBLIC_ORIGIN HTTP_PORT COMPOSE_PROJECT_NAME VITE_ICE_SERVERS
cd "$DEPLOY_DIR"
docker compose config --quiet
docker compose build --pull
docker compose up -d --wait --wait-timeout 180
docker compose ps
printf '\n部署完成：%s\n本机监听端口：%s\n部署目录：%s\n查看日志：cd %q && docker compose logs -f\n' \
    "$PUBLIC_ORIGIN" "$HTTP_PORT" "$DEPLOY_DIR" "$DEPLOY_DIR"
