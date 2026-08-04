try:
    from .adapter import register
except ImportError:  # 被当作顶层模块导入(如 pytest 根包收集、目录名非标识符)时退化为惰性
    register = None  # type: ignore[assignment]

__all__ = ["register"]
