
@pytest.fixture(scope='class')
def db():
    engine = create_engine(
        'sqlite:///:memory:',
        # 'sqlite:///./test.db',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool,
        # echo=True,
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    models.Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    notes_v2.crud.user.create(db, HTTPBasicCredentials(username='anon', password=''))
    yield db
    # models.Base.metadata.drop_all(bind=engine)
    db.close()
