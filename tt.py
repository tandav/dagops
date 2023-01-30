import dotenv
dotenv.load_dotenv()


from dagops.state import schemas




from dagops.state.crud.dag import dag_crud
from dagops.daemon import Daemon
from dagops.dependencies import get_db_cm

url = 'http://localhost:5002/api'
# url = 'https://dagops.tandav.me/api'

a = schemas.ShellTaskInputData(command=['ls'], env={'pass': 'a'}, worker_name='cpu')
b = schemas.ShellTaskInputData(command=['ls'], env={'pass': 'b'}, worker_name='cpu')
c = schemas.ShellTaskInputData(command=['ls'], env={'pass': 'c'}, worker_name='cpu')
d = schemas.ShellTaskInputData(command=['ls'], env={'pass': 'd'}, worker_name='cpu')
e = schemas.ShellTaskInputData(command=['ls'], env={'pass': 'e'}, worker_name='cpu')
tasks_input_data = [a, b, c, d, e]
dag = {
    a: [],
    b: [a],
    c: [a, b],
    d: [b, c],
    e: [d],
}
Daemon.validate_dag(dag)
dag = Daemon.prepare_dag(dag)

with get_db_cm() as db:
    dag_head_task = dag_crud.create(db, dag)
    print(dag_head_task.upstream)
