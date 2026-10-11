FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY services ./services
COPY shared ./shared
COPY protos ./protos
EXPOSE 5001 6001
CMD ["node", "services/cs/server.js"]
