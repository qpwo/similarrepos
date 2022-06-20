/**
 * The costar graph is highly connected, so a single-run updateStars and
 * updateGazers is probably adequate. A fancier back-and-forth stack-based
 * traversal would be slightly more complete but unnecessarily complicates the
 * database update logic.
 */

import { range, uniq } from 'lodash'
import { gazersdb, starsdb, statusdb } from './db'
import { getAllTargets } from './starpuller'

const WEEK = 7 * 24 * 60 * 60 * 1000
const expiredDate = new Date(Date.now() - WEEK)
const LOG_FREQUENCY = 1000
const ESTIMATED_MAX_RECORDS = 10_000_000
const NUM_PARALLEL_PULLERS = 5

async function main() {
    await updateEntireDb()
    // await updateCostars()
}

async function updateEntireDb() {
    log('STARTING')
    for (const _ in range(1000)) {
        await Promise.all([runDbBatch('gazers'), runDbBatch('stars')])
        log('SLEEPING TEN MINUTES')
        await sleep(1000 * 60 * 10)
    }
}

type Source = string
type Target = string
/** Find targets of missing or expired sources and update statusdb */
async function runDbBatch(mode: 'stars' | 'gazers'): Promise<void> {
    log('')
    const [sourceType, targetType, edgedb] =
        mode === 'stars'
            ? (['user', 'repo', starsdb] as const)
            : (['repo', 'user', gazersdb] as const)
    let numDiscovered = 0
    let numSucceed = 0
    let numFail = 0
    const sourcesGen = keyGenerator(sourceType, edgedb)
    await Promise.all(
        Array.from({ length: NUM_PARALLEL_PULLERS }, async () => {
            const targetsGen = getAllTargets({
                mode,
                sources: sourcesGen,
            })
            for await (const res of targetsGen) {
                if (res.type === 'complete') {
                    onComplete(res.source, res.targets)
                } else {
                    onFail(res.source)
                }
            }
        })
    )

    async function onComplete(source: Source, targets: Target[]) {
        // log('success:', { source, targets })
        numSucceed++
        logBatchProgress()
        let oldTargets: Target[] = []
        try {
            oldTargets = await edgedb.get(source)
        } catch {}
        const finalTargets = uniq([...oldTargets, ...targets])
        const char = source.includes('/') ? '.' : ','
        // dots are repos; commas are users; Xs are failures
        process.stdout.write(char)
        statusdb.put(source, {
            hadError: false,
            lastPulled: new Date().toISOString(),
            type: sourceType,
        })
        edgedb.put(source, finalTargets)
        // add new targets to status db so we will fetch them later
        const targetStatuses = await statusdb.getMany(targets)
        const b = statusdb.batch()
        for (const i of range(targets.length)) {
            if (targetStatuses[i] == null) {
                numDiscovered++
                b.put(targets[i], {
                    hadError: false,
                    lastPulled: false,
                    type: targetType,
                })
            }
        }
        b.write()
    }

    function logBatchProgress() {
        if ((numSucceed + numFail) % LOG_FREQUENCY === 0) {
            log(
                `\n\t ${mode}: ${numSucceed.toLocaleString()} success, ${numFail.toLocaleString()} fail, ${numDiscovered.toLocaleString()} discovered\n`
            )
        }
    }

    async function onFail(source: string) {
        // log('failure:', { source })
        numFail++
        logBatchProgress()
        process.stdout.write('X')
        statusdb.put(source, {
            hadError: true,
            lastPulled: new Date().toISOString(),
            type: sourceType,
        })
    }
}

async function* keyGenerator(
    sourceType: 'user' | 'repo',
    edgedb: typeof starsdb | typeof gazersdb
): AsyncGenerator<[string, string | undefined]> {
    let numSkipped = 0
    for await (const source of statusdb.keys()) {
        const status = await statusdb.get(source)
        if (
            status.type !== sourceType // || status.hadError
        )
            continue
        if (status.lastPulled && new Date(status.lastPulled) > expiredDate) {
            numSkipped++
            if (numSkipped % 100_000 === 0)
                log(`skipped ${numSkipped.toLocaleString()} ${sourceType}s`)
            continue
        }
        try {
            const targets = await edgedb.get(source)
            yield [source, targets.at(-1)]
        } catch {
            yield [source, undefined]
        }
    }
}

async function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

async function log(...args: any[]) {
    console.log(new Date(), ...args)
}

main()
