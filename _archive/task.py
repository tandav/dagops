import subprocess
import abc
import time
import random
from typing import Any



class ConstanntTask(Task):
    def __init__(self, deps: list[str], **kwargs):
        super().__init__(deps, **kwargs)
        self.value = kwargs['value']

    def run(self):
        time.sleep(2 * random.random())
        return self.value

class SumTask(Task):
    def __init__(self, deps: list[str], **kwargs):
        super().__init__(deps, **kwargs)
        self.a = kwargs['a']
        self.b = kwargs['b']

    def run(self):
        time.sleep(2 * random.random())
        return self.a + self.b


class ProdTask(Task):
    def __init__(self, deps: list[str], **kwargs):
        super().__init__(deps, **kwargs)
        self.a = kwargs['a']
        self.b = kwargs['b']

    def run(self):
        time.sleep(2 * random.random())
        return self.a * self.b


class ShellTask(Task):
    def __init__(
        self,
        deps: list[str],
        **kwargs,
        # command: list[str],
        # env: dict[str, str] | None = None,
    ):
        super().__init__(deps, **kwargs)
        self.command = kwargs['command']
        self.env = kwargs['env']
        # self.deps = deps

    @classmethod
    def from_path(cls, deps, path: str, env: dict[str, str] | None = None):
        return cls(deps, command=['sh', path], env=env)
        
    def run(self):
        subprocess.run(self.command, env=self.env)