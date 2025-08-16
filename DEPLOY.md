# Deploy to DigitalOcean

Simple manual deployment steps for Project Anthill.

## Server Info
- IP: `157.245.190.114`
- User: `root`
- Password: `SecurePassword2025Ape`
- Project Directory: `/opt/project-anthill`

## Deployment Steps

### 1. Clean server directory (if needed)
```bash
ssh root@157.245.190.114
rm -rf /opt/project-anthill/* /opt/project-anthill/.*
exit
```

### 2. Copy files from local machine
```cmd
scp -r * root@157.245.190.114:/opt/project-anthill/
```

### 3. Deploy on server
```bash
ssh root@157.245.190.114
cd /opt/project-anthill
docker-compose down
docker-compose up -d --build
docker-compose ps
```

### 4. Check logs (optional)
```bash
docker-compose logs -f reddit1
docker-compose logs -f stocktwits
```

## That's it!
Your scrapers should be running. The `docker-compose ps` command shows if all containers are up and running.
