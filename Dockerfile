FROM python:3.11-slim

# Install system dependencies for Playwright and cron
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    git \
    libgconf-2-4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libgtk-3-0 \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN python -m playwright install chromium
RUN python -m playwright install-deps

# Copy Python script
COPY seko_cycles_bigquery.py ./

# Create entrypoint script that handles secrets and starts cron
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "🚀 SEKO Cycles Container Starting at $(date)"\n\
echo "Python version: $(python --version)"\n\
echo "Playwright version: $(python -m playwright --version)"\n\
\n\
# Create .env file from Docker secret\n\
if [ -f "/run/secrets/seko_env_file" ]; then\n\
    cp /run/secrets/seko_env_file /app/.env\n\
    echo "✅ Environment file loaded from secret"\n\
else\n\
    echo "⚠️ Warning: seko_env_file secret not found"\n\
fi\n\
\n\
# Verify BigQuery credentials\n\
if [ -f "/run/secrets/bigquery_credentials" ]; then\n\
    echo "✅ BigQuery credentials found"\n\
else\n\
    echo "⚠️ Warning: bigquery_credentials secret not found"\n\
fi\n\
\n\
# Set up crontab if config exists\n\
if [ -f "/etc/cron.d/seko-cron" ]; then\n\
    crontab /etc/cron.d/seko-cron\n\
    echo "✅ Crontab configured"\n\
else\n\
    echo "⚠️ Warning: cron configuration not found"\n\
fi\n\
\n\
# Test run the script once on startup\n\
echo "🧪 Testing script execution..."\n\
python /app/seko_cycles_bigquery.py || echo "⚠️ Initial test run failed"\n\
\n\
# Start cron in foreground\n\
echo "⏰ Starting cron daemon..."\n\
cron -f' > /entrypoint.sh && chmod +x /entrypoint.sh

# Set timezone
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Set environment for Playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/app/pw-browsers

# Run the entrypoint script
ENTRYPOINT ["/entrypoint.sh"]