# v3_manager


```console

from scielo_v3_manager import manage

#manage.connect('0.0.0.0', '27017', schema='meu_opac', login='', password='')
manage.connect("mongodb://0.0.0.0:27017/opac_br")
manage.register(doi='doi', filename='filename', v2='v2', aop='aop', v3='v3', status='status', v1= 'v1', others=['x', 'y'], fields={'f': 'b'})

```
