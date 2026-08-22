FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
COPY proto /app/proto
CMD ["npm","start"]
