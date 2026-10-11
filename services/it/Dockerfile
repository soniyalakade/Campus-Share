FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY services ./services
COPY shared ./shared
COPY protos ./protos
EXPOSE 5002 6002
CMD ["node", "services/it/server.js"]
