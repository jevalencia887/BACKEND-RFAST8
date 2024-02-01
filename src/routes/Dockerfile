FROM node:18-bookworm-slim

WORKDIR /medicarte-app

RUN mkdir -p /documents

COPY package*.json ./

RUN npm install --omit=dev

RUN npm install -g sequelize-cli

COPY . .

EXPOSE 3000