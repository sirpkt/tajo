select a.id, b.name from table2 b join table1 a on a.id = b.id group by a.id, b.name order by a.id;