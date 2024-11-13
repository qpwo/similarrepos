import { appendFileSync, createWriteStream, rmSync } from 'fs'
import * as lev from '../update/db'
import { BigMap } from './bigmap'
const MAX_ROWS = 10_000
const LOG_FREQUENCY = 50

const BATCH_SIZE = 1000
// a Map can't do more than about 17 million entries apparently
const idOf = new BigMap() as Map<string, number>
// const idOf: Record<string, number> = {}
// const nameOf = new Map<number, string>()
let nextId = 1
function getId(name: string) {
    const has = idOf.get(name)
    // const has = idOf[name]
    if (has) return has
    nextId += 1
    idOf.set(name, nextId)
    // idOf[name] = nextId
    // nameOf.set(nextId, name)
    return nextId
}

const dir = 'migration/csv/'
async function main() {
    let path = dir + 'statuses.csv'
    try {
        rmSync(path)
    } catch {}
    let stream = createWriteStream(path)
    console.log('path:', path, '---------------------')
    stream.write('id,last_pulled,had_error,is_user\n')
    let count = 0
    for await (const [name, val] of lev.statusdb.iterator()) {
        if (count++ > MAX_ROWS) break
        if (count % LOG_FREQUENCY === 0)
            console.error(
                new Date().toLocaleTimeString(),
                count.toLocaleString()
            )
        getId(name)
        stream.write(
            getId(name) +
                ',' +
                (val.lastPulled ? new Date(val.lastPulled).getTime() : null) +
                ',' +
                val.hadError +
                ',' +
                (val.type === 'user') +
                '\n'
        )
    }
    stream.end()

    path = dir + 'stars.csv'
    stream = createWriteStream(path)
    try {
        rmSync(path)
    } catch {}
    console.log('path:', '---------------------')
    stream.write('user_id,repo_id\n')
    count = 0
    for await (const [user, repos] of lev.starsdb.iterator()) {
        const userId = getId(user)
        if (count++ > MAX_ROWS) break
        if (count % LOG_FREQUENCY === 0)
            console.error(
                new Date().toLocaleTimeString(),
                count.toLocaleString()
            )
        for (const repo of repos) {
            stream.write(userId + ',' + getId(repo) + '\n')
        }
    }
    stream.end()

    path = dir + `names.csv`
    stream = createWriteStream(path)
    console.log('writing names')
    try {
        rmSync(path)
    } catch {}
    stream.write('id,name\n')
    count = 0
    for (const map of idOf.maps) {
        for (const [name, id] of map.entries()) {
            stream.write(id + ',' + name + '\n')
            count++
            if (count % LOG_FREQUENCY === 0)
                console.error(
                    new Date().toLocaleTimeString(),
                    count.toLocaleString()
                )
        }
    }
    stream.end()

    console.error(new Date().toLocaleTimeString(), 'All done!')
}

function setOutput(filename: string) {
    const path = `./migration/csv/${filename}`
    try {
        rmSync(path)
    } catch {}
    console.error(`writing to ${path}`)
    const access = createWriteStream(path)
    process.stdout.write = access.write.bind(access)
}

void main()
