from sqlalchemy import create_engine, text, select, BigInteger, exists
from sqlalchemy.engine import URL
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm import Session

from dotenv import dotenv_values, set_key

from typing import Optional, List

CONFIG = dotenv_values(".env")  # CONFIG = {"USER": "foo", "EMAIL": "foo@example.org"}

    # declarative base class
class Base(DeclarativeBase):
    pass

class EmpregadoProjeto(Base):
    __tablename__ = "Empregado_Projeto"
    __table_args__ = {"schema": CONFIG['POSTGRES_SCHEMA']}
    empregado_id: Mapped[int] = mapped_column(ForeignKey(f"{CONFIG['POSTGRES_SCHEMA']}.Empregado.id"), primary_key=True) # {CONFIG['POSTGRES_SCHEMA']}.Empregado.id (schema.table.column)
    projeto_id: Mapped[int] = mapped_column(BigInteger, ForeignKey(f"{CONFIG['POSTGRES_SCHEMA']}.Projeto.id"), primary_key=True) # {CONFIG['POSTGRES_SCHEMA']}.Projeto.id (schema.table.column)
    obsevacao: Mapped[Optional[str]]
    projeto: Mapped["Projeto"] = relationship(back_populates="empregados")
    empregado: Mapped["Empregado"] = relationship(back_populates="projetos")

# an example mapping using the base
class Empregado(Base):
    __tablename__ = "Empregado"
    __table_args__ = {"schema": CONFIG['POSTGRES_SCHEMA']}
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    projetos: Mapped[List["EmpregadoProjeto"]] = relationship(back_populates="empregado")

class Projeto(Base):
    __tablename__ = "Projeto"
    __table_args__ = {"schema": CONFIG['POSTGRES_SCHEMA']}
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    empregados: Mapped[List["EmpregadoProjeto"]] = relationship(back_populates="projeto")

def init_engine():
    url = URL.create(
        drivername="postgresql",
        username= CONFIG['POSTGRES_USER'],
        password= CONFIG['POSTGRES_PW'],
        host="localhost",
        port=5432,
        database="postgres"
    )
    
    engine = create_engine(url)



    return engine

def gen_schema():

    engine = init_engine()

    # criacao da base
    
    Base.metadata.create_all(engine)

    set_key('.env', 'FIRST_EXECUTION', 'nao')


def gen_empregado(emp_nome, proj_nome, proj_obs):
        empregado_projeto = EmpregadoProjeto()
        emp = Empregado(name=emp_nome)
        projeto_a = Projeto(name=proj_nome)
        empregado_projeto.empregado = emp
        empregado_projeto.projeto = projeto_a
        empregado_projeto.obsevacao = proj_obs
        return emp

if __name__ == '__main__':

    if CONFIG['FIRST_EXECUTION'] == 'sim':
        gen_schema()

    engine = init_engine()

    # criar um objeto na base
    with Session(engine) as session:
        emp = gen_empregado('jose', 'alpha', 'secreto')
        session.add(emp)
        # res = session.query(Projeto).filter(Projeto.name=='projeto A')
        res = session.query(Projeto)
        res2= session.query(exists().where(Projeto.name=='alpha'))
        print(res.statement)
        print(res2.statement)
        for r in res:
            print(f'\n{r.id} - {r.name}')
        
        print(res2.scalar())

        print('existe') if res2.scalar() else print('nao existe!!!!')        #stmt = select(User).where(User.name.in_(["spongebob", "sandy"]))

        session.commit()
    pass
