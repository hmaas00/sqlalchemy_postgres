from sqlalchemy import create_engine, text, select
from sqlalchemy.engine import URL
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm import Session

from dotenv import dotenv_values

from typing import Optional, List




if __name__ == '__main__':
    config = dotenv_values(".env")  # config = {"USER": "foo", "EMAIL": "foo@example.org"}

    url = URL.create(
        drivername="postgresql",
        username= config['POSTGRES_USER'],
        password= config['POSTGRES_PW'],
        host="localhost",
        port=5432,
        database="postgres"
    )

    # declarative base class
    class Base(DeclarativeBase):
        pass

    class Empregado_Projeto(Base):
        __tablename__ = "Empregado_Projeto"
        __table_args__ = {"schema": 'mytest'}
        empregado_id: Mapped[int] = mapped_column(ForeignKey("mytest.Empregado.id"), primary_key=True) # mytest.Empregado.id (schema.table.column)
        projeto_id: Mapped[int] = mapped_column(ForeignKey("mytest.Projeto.id"), primary_key=True) # mytest.Projeto.id (schema.table.column)
        obsevacao: Mapped[Optional[str]]
        projeto: Mapped["Projeto"] = relationship(back_populates="empregados")
        empregado: Mapped["Empregado"] = relationship(back_populates="projetos")
    
    # an example mapping using the base
    class Empregado(Base):
        __tablename__ = "Empregado"
        __table_args__ = {"schema": 'mytest'}
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(30))
        projetos: Mapped[List["Empregado_Projeto"]] = relationship(back_populates="empregado")

    class Projeto(Base):
        __tablename__ = "Projeto"
        __table_args__ = {"schema": 'mytest'}
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(30))
        empregados: Mapped[List["Empregado_Projeto"]] = relationship(back_populates="projeto")

    # criacao da base
    engine = create_engine(url)
    Base.metadata.create_all(engine)


    def gen_empregado(emp_nome, proj_nome, proj_obs):
        empregado_projeto = Empregado_Projeto()
        emp = Empregado(name=emp_nome)
        projeto_a = Projeto(name=proj_nome)
        empregado_projeto.empregado = emp
        empregado_projeto.projeto = projeto_a
        empregado_projeto.obsevacao = proj_obs
        return emp


    # criar um objeto na base
    with Session(engine) as session:
        # emp = gen_empregado('jose', 'alpha', 'secreto')
        # session.add(emp)
        res = session.execute(text('SELECT id, nome FROM mytest."OutAlchemy";'))
        for r in res:
            print(f'\n{r}')

        #stmt = select(User).where(User.name.in_(["spongebob", "sandy"]))

        # session.commit()
    pass
