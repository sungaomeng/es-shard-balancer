# 使用 Python 3.9 作为基础镜像
FROM python:3.9-bullseye

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONUNBUFFERED=1

# 安装必要的依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制脚本文件
COPY shard_balancer.py .

# 运行服务
CMD ["python", "shard_balancer.py"]
