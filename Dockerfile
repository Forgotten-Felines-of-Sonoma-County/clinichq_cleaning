FROM python:3.12-slim

WORKDIR /app

# Copy minimal requirements and install first (better cache)
COPY requirements_streamlit.txt ./
RUN pip install --no-cache-dir -r requirements_streamlit.txt

# Copy app code
COPY streamlit_dashboard.py ./

# Streamlit config via env
ENV PORT=8080 \
    STREAMLIT_SERVER_PORT=8080 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    PYTHONUNBUFFERED=1

# Expose port for local runs
EXPOSE 8080

# Start streamlit; Cloud Run will inject env vars
CMD ["streamlit", "run", "streamlit_dashboard.py", "--server.port=8080", "--server.address=0.0.0.0"]
