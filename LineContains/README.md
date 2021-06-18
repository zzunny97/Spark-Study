## LineContains 어플리케이션

### Description
- [Apache Spark Quick Start 문서](https://spark.apache.org/docs/latest/quick-start.html) 를 직접 따라해서 만든 어플리케이션
- 해당 디렉토리에 있는 README.md 파일을 읽어,
  - 문자열 "a"가 포함된 라인의 개수
  - 문자열 "b"가 포함된 라인의 개수를 출력해주는 어플리케이션
-----
### How to run

```shell
# Package a jar containing your application
sbt package

# Use spark-submit to run your application (--master 옵션은 optional)
YOUR_SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master local[4] \
  target/scala-2.12/simple-project_2.12-1.0.jar 

```
