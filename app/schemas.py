from pydantic import BaseModel, Field, field_validator


class SampleIn(BaseModel):
    # 两值必填且非空（与旧表 NOT NULL 约束一致）
    temp: float = Field(allow_inf_nan=False)
    ph: float = Field(allow_inf_nan=False)


class SampleOut(BaseModel):
    id: int
    temp: float
    ph: float


class ArchiveOrderIn(BaseModel):
    # 作废单号：业务方提供的唯一凭证，同单号重试返回首次结果
    order_no: str = Field(min_length=1, max_length=128)
    sample_ids: list[int] = Field(min_length=1)

    @field_validator("order_no")
    @classmethod
    def _strip_order_no(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("order_no 不能为空")
        return value

    @field_validator("sample_ids")
    @classmethod
    def _positive_ids(cls, value: list[int]) -> list[int]:
        if any(sid <= 0 for sid in value):
            raise ValueError("sample_ids 必须为正整数")
        return value
