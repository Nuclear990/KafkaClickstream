FROM python:3.10

WORKDIR /app

# Copy only requirements first (for caching)
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy rest of the code
COPY . .

CMD ["python3"]