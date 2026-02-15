#!/bin/bash
# =====================================================
# Taco Group Dashboard - Production Setup Script
# Author: Hassan
# =====================================================

# 1. Update and upgrade server
sudo apt update -y && sudo apt upgrade -y

# 2. Install essential packages
sudo apt install -y python3 python3-pip python3-venv git nginx

# 3. Create project folder (if not exists)
PROJECT_DIR="/root/taco_dashboard"
mkdir -p $PROJECT_DIR

cd $PROJECT_DIR

# 4. Setup Python virtual environment
python3 -m venv venv
source venv/bin/activate

# 5. Upgrade pip
pip install --upgrade pip

# 6. Install Python dependencies
pip install fastapi uvicorn sqlalchemy pydantic

# If requirements.txt exists, install from it
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
fi

# 7. Create systemd service for 24/7 auto-run
SERVICE_FILE="/etc/systemd/system/taco_dashboard.service"

sudo bash -c "cat > $SERVICE_FILE" <<EOL
[Unit]
Description=Taco Group Live Production Dashboard
After=network.target

[Service]
User=root
WorkingDirectory=$PROJECT_DIR
ExecStart=$PROJECT_DIR/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8001
Restart=always

[Install]
WantedBy=multi-user.target
EOL

# 8. Reload systemd, enable and start service
sudo systemctl daemon-reload
sudo systemctl enable taco_dashboard
sudo systemctl start taco_dashboard

# 9. Check status
sudo systemctl status taco_dashboard --no-pager

echo "✅ Taco Group Dashboard deployed successfully on port 8001!"
echo "Access it at http://<YOUR_SERVER_IP>:8001"
