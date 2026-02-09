#!/bin/bash
# install.sh
#
# Email Scraper VPS Deployment Script
# Run this on your VPS to set up the production environment
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/your-org/email-scraper/main/deploy/install.sh | bash
#   OR
#   ./install.sh
#
# Prerequisites:
#   - Ubuntu 20.04+ / Debian 11+
#   - sudo access
#   - Git installed

set -e

# Configuration
APP_NAME="email-scraper"
APP_DIR="/opt/${APP_NAME}"
DEPLOY_USER="deploy"
PYTHON_VERSION="3.11"
NUM_WORKERS=2

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# Check we're running as root or with sudo
check_privileges() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root or with sudo"
    fi
}

# Install system dependencies
install_system_deps() {
    log_info "Installing system dependencies..."
    
    apt-get update
    apt-get install -y \
        python${PYTHON_VERSION} \
        python${PYTHON_VERSION}-venv \
        python${PYTHON_VERSION}-dev \
        python3-pip \
        redis-server \
        postgresql \
        postgresql-contrib \
        nginx \
        git \
        curl \
        htop \
        jq
    
    log_success "System dependencies installed"
}

# Create deploy user
create_deploy_user() {
    log_info "Creating deploy user..."
    
    if id "$DEPLOY_USER" &>/dev/null; then
        log_warn "User $DEPLOY_USER already exists"
    else
        useradd -m -s /bin/bash "$DEPLOY_USER"
        log_success "User $DEPLOY_USER created"
    fi
    
    # Add to sudo group for service management
    usermod -aG sudo "$DEPLOY_USER"
    
    # Allow passwordless sudo for systemctl commands only
    cat > /etc/sudoers.d/email-scraper << 'EOF'
deploy ALL=(ALL) NOPASSWD: /bin/systemctl start email-scraper*
deploy ALL=(ALL) NOPASSWD: /bin/systemctl stop email-scraper*
deploy ALL=(ALL) NOPASSWD: /bin/systemctl restart email-scraper*
deploy ALL=(ALL) NOPASSWD: /bin/systemctl status email-scraper*
deploy ALL=(ALL) NOPASSWD: /bin/systemctl enable email-scraper*
deploy ALL=(ALL) NOPASSWD: /bin/systemctl disable email-scraper*
deploy ALL=(ALL) NOPASSWD: /bin/systemctl daemon-reload
deploy ALL=(ALL) NOPASSWD: /bin/journalctl -u email-scraper*
EOF
    chmod 440 /etc/sudoers.d/email-scraper
    
    log_success "Deploy user configured"
}

# Setup application directory
setup_app_dir() {
    log_info "Setting up application directory..."
    
    mkdir -p "$APP_DIR"/{data,logs,bin}
    
    # Clone or update repo
    if [[ -d "$APP_DIR/src" ]]; then
        log_info "Updating existing installation..."
        cd "$APP_DIR"
        git pull origin main || true
    else
        log_info "Cloning repository..."
        # Replace with your actual repo URL
        git clone https://github.com/your-org/email-scraper.git "$APP_DIR/repo-temp" || {
            log_warn "Git clone failed - assuming manual deployment"
        }
        if [[ -d "$APP_DIR/repo-temp" ]]; then
            mv "$APP_DIR/repo-temp"/* "$APP_DIR/"
            rm -rf "$APP_DIR/repo-temp"
        fi
    fi
    
    # Set ownership
    chown -R "$DEPLOY_USER:$DEPLOY_USER" "$APP_DIR"
    
    log_success "Application directory ready"
}

# Setup Python virtual environment
setup_venv() {
    log_info "Setting up Python virtual environment..."
    
    cd "$APP_DIR"
    
    # Create venv if doesn't exist
    if [[ ! -d ".venv" ]]; then
        python${PYTHON_VERSION} -m venv .venv
    fi
    
    # Activate and install dependencies
    source .venv/bin/activate
    pip install --upgrade pip wheel
    pip install -r requirements.txt
    
    chown -R "$DEPLOY_USER:$DEPLOY_USER" "$APP_DIR/.venv"
    
    log_success "Python environment ready"
}

# Setup Redis
setup_redis() {
    log_info "Configuring Redis..."
    
    # Enable and start Redis
    systemctl enable redis-server
    systemctl start redis-server
    
    # Wait for Redis to be ready
    for i in {1..10}; do
        if redis-cli ping &>/dev/null; then
            log_success "Redis is running"
            return 0
        fi
        sleep 1
    done
    
    log_error "Redis failed to start"
}

# Setup PostgreSQL (optional - can use SQLite)
setup_postgres() {
    log_info "Configuring PostgreSQL..."
    
    systemctl enable postgresql
    systemctl start postgresql
    
    # Create database and user
    sudo -u postgres psql -c "CREATE USER email_scraper WITH PASSWORD 'changeme';" 2>/dev/null || true
    sudo -u postgres psql -c "CREATE DATABASE email_scraper OWNER email_scraper;" 2>/dev/null || true
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE email_scraper TO email_scraper;" 2>/dev/null || true
    
    log_success "PostgreSQL configured"
    log_warn "Remember to change the database password in .env!"
}

# Install systemd services
install_services() {
    log_info "Installing systemd services..."
    
    # Copy service files
    cp "$APP_DIR/deploy/email-scraper-api.service" /etc/systemd/system/
    cp "$APP_DIR/deploy/email-scraper-worker@.service" /etc/systemd/system/
    
    # Reload systemd
    systemctl daemon-reload
    
    # Enable services
    systemctl enable email-scraper-api
    for i in $(seq 1 $NUM_WORKERS); do
        systemctl enable "email-scraper-worker@${i}"
    done
    
    log_success "Systemd services installed"
}

# Install management script
install_esctl() {
    log_info "Installing management script..."
    
    cp "$APP_DIR/deploy/esctl" "$APP_DIR/bin/esctl"
    chmod +x "$APP_DIR/bin/esctl"
    
    # Create symlink in /usr/local/bin
    ln -sf "$APP_DIR/bin/esctl" /usr/local/bin/esctl
    
    log_success "esctl installed (available as 'esctl' command)"
}

# Setup environment file
setup_env() {
    log_info "Setting up environment file..."
    
    if [[ ! -f "$APP_DIR/.env" ]]; then
        cp "$APP_DIR/deploy/.env.production.template" "$APP_DIR/.env"
        chown "$DEPLOY_USER:$DEPLOY_USER" "$APP_DIR/.env"
        chmod 600 "$APP_DIR/.env"
        log_warn "Created .env from template - EDIT THIS FILE BEFORE STARTING!"
    else
        log_info ".env already exists, skipping"
    fi
}

# Setup Nginx reverse proxy (optional)
setup_nginx() {
    log_info "Configuring Nginx reverse proxy..."
    
    cat > /etc/nginx/sites-available/email-scraper << 'EOF'
server {
    listen 80;
    server_name _;  # Replace with your domain
    
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
        proxy_read_timeout 300;
        proxy_connect_timeout 300;
        proxy_send_timeout 300;
    }
}
EOF
    
    ln -sf /etc/nginx/sites-available/email-scraper /etc/nginx/sites-enabled/
    rm -f /etc/nginx/sites-enabled/default
    
    nginx -t && systemctl reload nginx
    
    log_success "Nginx configured"
}

# Apply database schema
apply_schema() {
    log_info "Applying database schema..."
    
    cd "$APP_DIR"
    source .venv/bin/activate
    
    # Run schema application script if it exists
    if [[ -f "src/db/apply_schema.py" ]]; then
        python -m src.db.apply_schema || log_warn "Schema application had issues"
    fi
    
    log_success "Database schema applied"
}

# Print summary
print_summary() {
    echo ""
    echo "============================================================"
    echo -e "${GREEN}Email Scraper Installation Complete!${NC}"
    echo "============================================================"
    echo ""
    echo "Important next steps:"
    echo ""
    echo "1. Edit the environment file:"
    echo "   sudo -u deploy nano $APP_DIR/.env"
    echo ""
    echo "2. Start the services:"
    echo "   esctl all start"
    echo ""
    echo "3. Check health:"
    echo "   esctl health"
    echo ""
    echo "Quick commands:"
    echo "   esctl api start|stop|restart|logs"
    echo "   esctl worker start|stop|restart|logs"
    echo "   esctl scale 3        # Scale to 3 workers"
    echo "   esctl health         # Health check"
    echo ""
    echo "Remote commands (from your local machine):"
    echo "   ssh ${DEPLOY_USER}@your-vps 'esctl api restart'"
    echo "   ssh ${DEPLOY_USER}@your-vps 'esctl worker restart'"
    echo "   ssh ${DEPLOY_USER}@your-vps 'esctl health'"
    echo ""
    echo "Logs:"
    echo "   sudo journalctl -u email-scraper-api -f"
    echo "   sudo journalctl -u email-scraper-worker@1 -f"
    echo ""
    echo "============================================================"
}

# Main installation flow
main() {
    echo ""
    echo "============================================================"
    echo "Email Scraper VPS Deployment"
    echo "============================================================"
    echo ""
    
    check_privileges
    install_system_deps
    create_deploy_user
    setup_app_dir
    setup_venv
    setup_redis
    # setup_postgres  # Uncomment if using PostgreSQL
    setup_env
    install_services
    install_esctl
    setup_nginx
    # apply_schema  # Uncomment after .env is configured
    
    print_summary
}

# Run main
main "$@"
