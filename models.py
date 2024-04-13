from pydantic import BaseModel, HttpUrl, field_validator


class ValidateLink(BaseModel):
    title: str
    url: str

    @field_validator('url')
    def validate_link(cls, v: HttpUrl) -> HttpUrl:
        if not v.startswith('https'):
            raise ValueError('URL must start with https')
        if 'dexscreener.com' not in v:
            raise ValueError('URL must contain dexscreener.com')
        if 'new-pairs' not in v:
            raise ValueError('URL must include new-pairs segment')
        return v
