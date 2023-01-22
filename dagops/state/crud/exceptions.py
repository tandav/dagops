from http import HTTPStatus

from fastapi import HTTPException


class CrudError(BaseException):
    STATUS = None

    @classmethod
    @property
    def http(cls):
        return HTTPException(status_code=cls.STATUS, detail=cls.__name__)


class HttpNotFound(CrudError):
    STATUS = HTTPStatus.NOT_FOUND


class HttpBadRequest(CrudError):
    STATUS = HTTPStatus.BAD_REQUEST


class HttpUnauthorized(CrudError):
    STATUS = HTTPStatus.UNAUTHORIZED


# class TagNotExistsError(HttpNotFound): pass
# class UserNotExistsError(HttpNotFound): pass
# class NoteNotExistsError(HttpNotFound): pass
# class NoteAlreadyArchived(HttpBadRequest): pass
# class NoteAlreadyUnarchived(HttpBadRequest): pass
# class TagAlreadyExists(HttpBadRequest): pass
# class AnonNotesCantBeUpdated(HttpUnauthorized): pass
# class AnonNotesCantBePrivate(HttpUnauthorized): pass
# class UserIsNotAllowedToEditOtherUserNotes(HttpUnauthorized): pass
# class ColorForNullTag(HttpBadRequest): pass
