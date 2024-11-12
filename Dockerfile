# Use an official Python runtime as a parent image
FROM python:3.12

# Set the working directory inside the container
WORKDIR /app

COPY requirements.txt /app/
COPY . /app/

# Install system dependencies required for the application
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install Playwright dependencies (playwright needs some browsers to be installed)
RUN python -m playwright install

# Other dependecies
RUN python -m nltk.downloader -d /usr/local/share/nltk_data all


CMD ["python", "main.py"]
