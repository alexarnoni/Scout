import sys
sys.path.insert(0, 'backend')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()
rows = db.execute(text("""
    SELECT position, COUNT(*) as total 
    FROM players 
    WHERE position IS NOT NULL 
    GROUP BY position 
    ORDER BY total DESC
""")).fetchall()
for r in rows:
    print(r)
db.close()