# v3_manager


```python
import os

from scielo_v3_manager.v3_gen import generates
from scielo_v3_manager.pid_manager import Manager

local = "postgresql+psycopg2://user@localhost:5432/pid_manager"

manager = Manager(local, 20000)

done = manager.manage(
                v2=v2,
                v3=None,
                aop=aop,
                filename=os.path.basename(filename),
                doi=doi,
                status="active",
                generate_v3=generates,
            )
```
