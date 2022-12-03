import asyncio
import sys


async def number(n: int):
    # p = await asyncio.create_subprocess_exec('echo', f'from-echo-{n}')
    p = await asyncio.create_subprocess_exec(sys.executable, 'code.py', f'{n}')
    await p.communicate()
    print(p.returncode)
    return p
    # out = await p.wait()
    # return out
    # return p.stdout
    # await asyncio.sleep(random.random())
    # return n


async def main():
    tasks = [asyncio.create_task(number(i)) for i in range(10)]
    done, running = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
    for task in done:
        print('>', task.result())


if __name__ == '__main__':
    asyncio.run(main())
