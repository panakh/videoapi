# Use an official Node.js runtime as a parent image
FROM node:18-alpine

# Install FFmpeg and fonts
RUN apk add --no-cache ffmpeg fontconfig ttf-dejavu

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json (or npm-shrinkwrap.json)
COPY package*.json ./

# Install dependencies
# Using npm ci for cleaner installs, and --only=production for smaller image size
RUN npm ci --only=production

# Bundle app source
COPY . .

# Make port 3000 available to the world outside this container
EXPOSE 3000

# Define environment variable if needed (e.g., for PORT, though it defaults in server.js)
# ENV PORT=3000

# Run the app when the container launches
CMD ["node", "server.js"] 