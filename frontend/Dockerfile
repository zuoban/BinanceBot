FROM node:22.23.4-bookworm-slim AS build
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --ignore-scripts
COPY frontend/ ./
COPY Cargo.toml /Cargo.toml
RUN npm run build && find dist -type f \( -name '*.js' -o -name '*.css' -o -name '*.svg' \) -exec gzip -9 -k {} +

FROM nginx:1.31-alpine
COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD wget -Y off -q -O /dev/null http://127.0.0.1:80/theme-init.js || exit 1
