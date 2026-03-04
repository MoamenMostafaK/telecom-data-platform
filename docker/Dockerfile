FROM node:alpine

# 2. Create an app directory inside the container
WORKDIR /usr/src/app

# 3. Copy dependency files and install them
COPY package*.json ./
RUN npm install --production

# 4. Copy your actual application code
COPY . .

# 5. The port your app runs on
EXPOSE 3000

# 6. Command to start your app
CMD ["node", "index.js"]