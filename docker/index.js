const { Client } = require('pg');

// Note: Use 'db' as the host because that is the service name in your compose file
const client = new Client({
  connectionString: 'postgres://admin:admin@db:5432/telecomDB'
});

async function testConnection() {
  try {
    await client.connect();
    console.log("✅ Successfully connected to PostgreSQL!");
    
    const res = await client.query('SELECT NOW()');
    console.log("🕒 Database Time:", res.rows[0].now);
    
    await client.end();
  } catch (err) {
    console.error("❌ Connection error:", err.stack);
    process.exit(1);
  }
}

testConnection();
