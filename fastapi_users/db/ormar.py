from typing import Optional, Type

from pydantic import UUID4

from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.models import UD

import databases
import sqlalchemy

import ormar
from ormar import NoMatch

database = databases.Database("sqlite:///db.sqlite")
metadata = sqlalchemy.MetaData()


class OrmarBaseUserModel(ormar.Model):
    __abstract__ = True

    class Meta:
        database = database
        metadata = metadata

    id: ormar.UUID(primary_key=True)
    email = ormar.String(index=True, unique=True, nullable=False, max_length=255)
    hashed_password = ormar.CharField(null=False, max_length=255)
    is_active = ormar.Boolean(default=True, nullable=False)
    is_superuser = ormar.Boolean(default=False, nullable=False)

    async def to_dict(self):
        d = {}
        for field in self._meta.db_fields:
            d[field] = getattr(self, field)
        for field in self._meta.backward_fk_fields:
            d[field] = await getattr(self, field).all().values()
        return d


class OrmarBaseOAuthAccountModel(ormar.Model):
    __abstract__ = True

    class Meta:
        database = database
        metadata = metadata

    id = ormar.UUID(primary_key=True, max_length=255)
    oauth_name = ormar.String(nullable=False, max_length=255)
    access_token = ormar.String(nullable=False, max_length=255)
    expires_at = ormar.Integer(nullable=False)
    refresh_token = ormar.String(nullable=True, max_length=255)
    account_id = ormar.String(index=True, nullable=False, max_length=255)
    account_email = ormar.String(nullable=False, max_length=255)


class OrmarUserDatabase(BaseUserDatabase[UD]):
    """
    Database adapter for Ormar ORM.

    :param user_db_model: Pydantic model of a DB representation of a user.
    :param model: Ormar ORM model.
    :param oauth_account_model: Optional Ormar ORM model of a OAuth account.
    """

    model: Type[OrmarBaseUserModel]
    oauth_account_model: Optional[Type[OrmarBaseOAuthAccountModel]]

    def __init__(
        self,
        user_db_model: Type[UD],
        model: Type[OrmarBaseUserModel],
        oauth_account_model: Optional[Type[OrmarBaseOAuthAccountModel]] = None,
    ):
        super().__init__(user_db_model)
        self.model = model
        self.oauth_account_model = oauth_account_model

    async def get(self, id: UUID4) -> Optional[UD]:
        try:
            query = self.model.objects

            if self.oauth_account_model is not None:
                query = query.prefetch_related("oauth_accounts")

            user = await query.get(id=id)
            user_dict = await user.to_dict()

            return self.user_db_model(**user_dict)
        except NoMatch:
            return None

    async def get_by_email(self, email: str) -> Optional[UD]:
        query = self.model.objects.filter(email__iexact=email)
        if self.oauth_account_model is not None:
            query = query.prefetch_related("oauth_accounts")

        user = await query.first()

        if user is None:
            return None

        user_dict = await user.to_dict()
        return self.user_db_model(**user_dict)

    async def get_by_oauth_account(self, oauth: str, account_id: str) -> Optional[UD]:
        try:
            query = self.model.objects.prefetch_related("oauth_accounts").get(
                oauth_accounts__oauth_name=oauth, oauth_accounts__account_id=account_id
            )

            user = await query
            user_dict = await user.to_dict()

            return self.user_db_model(**user_dict)
        except NoMatch:
            return None

    async def create(self, user: UD) -> UD:
        user_dict = user.dict()
        oauth_accounts = user_dict.pop("oauth_accounts", None)

        model = self.model(**user_dict)
        await model.save()

        if oauth_accounts and self.oauth_account_model:
            oauth_account_objects = []
            for oauth_account in oauth_accounts:
                oauth_account_objects.append(
                    self.oauth_account_model(user=model, **oauth_account)
                )
            await self.oauth_account_model.objects.bulk_create(oauth_account_objects)

        return user

    async def update(self, user: UD) -> UD:
        user_dict = user.dict()
        user_dict.pop("id")  # Tortoise complains if we pass the PK again
        oauth_accounts = user_dict.pop("oauth_accounts", None)

        model = await self.model.get(id=user.id)
        for field in user_dict:
            setattr(model, field, user_dict[field])
        await model.save()

        if oauth_accounts and self.oauth_account_model:
            await model.oauth_accounts.all().delete()
            oauth_account_objects = []
            for oauth_account in oauth_accounts:
                oauth_account_objects.append(
                    self.oauth_account_model(user=model, **oauth_account)
                )
            await self.oauth_account_model.objects.bulk_create(oauth_account_objects)

        return user

    async def delete(self, user: UD) -> None:
        await self.model.objects.filter(id=user.id).delete()
