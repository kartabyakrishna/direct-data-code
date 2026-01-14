FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY veeva_accelerator/ ./veeva_accelerator/

# Add current directory to PYTHONPATH so local imports work
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Entry point
ENTRYPOINT ["python", "veeva_accelerator/accelerators/redshift/accelerator.py"]
