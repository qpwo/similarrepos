import {
  createPool, sql,
} from 'slonik'

async function main() {
  const db = await createPool('postgresql://root:root@localhost:5432/root')
  const res = await db.any(sql`SELECT * FROM ok`)
  console.log(res)
}

void main()
