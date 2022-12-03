import asyncio
import sys

queue = asyncio.Queue()

# read output from process in an infinite loop and
# put it in a queue


async def process_output(queue, key):
    proc = await asyncio.create_subprocess_exec(
        sys.executable, '-u', 'write_to_mongo.py',
        env={'TASK_NAME': key},
        stdout=asyncio.subprocess.PIPE,
    )

    # async for line in proc.stdout: # not works
    # queue.put_nowait((key, line))

    while not proc.stdout.at_eof():
        line = await proc.stdout.readline()
        await queue.put((key, line))
    print('proc.returncode:', proc.returncode)


async def main():
    # create multiple workers that run in parallel and pour
    # data from multiple sources into the same queue
    tasks = [
        asyncio.create_task(process_output(queue, 'task1')),
        asyncio.create_task(process_output(queue, 'task2')),
    ]

    while not all(task.done() for task in tasks):
        key, output = await queue.get()
        print('*', key, output)
        # if identifier == 'top':
        # ...

# import asyncio
# import sys

# async def main():
#     code = 'import datetime; print(datetime.datetime.now())'

#     # Create the subprocess; redirect the standard output
#     # into a pipe.
#     key = '42'
#     proc = await asyncio.create_subprocess_exec(
#         sys.executable, '-u', 'write_to_mongo.py',
#         env={'TASK_NAME': key},
#         stdout=asyncio.subprocess.PIPE,
#     )
#     # Read one line of output.
#     while True:
#         data = await proc.stdout.readline()
#         line = data.decode('ascii').rstrip()
#         print(line, proc.returncode)
#     # Wait for the subprocess exit.
#     await proc.wait()
#     return line


if __name__ == '__main__':
    asyncio.run(main())
