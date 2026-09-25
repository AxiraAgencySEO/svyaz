FROM node:22-alpine
WORKDIR /app
RUN npm install -g pnpm@11.19.0
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --prod --frozen-lockfile
COPY server ./server
COPY public ./public
ENV PORT=8787
EXPOSE 8787
USER node
CMD ["node", "server/index.mjs"]
