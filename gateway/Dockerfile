FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY gateway ./gateway
COPY shared ./shared
COPY protos ./protos
EXPOSE 4000
CMD ["node", "gateway/server.js"]
