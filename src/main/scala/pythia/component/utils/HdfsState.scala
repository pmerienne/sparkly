package pythia.component.utils

import pythia.config.PythiaConfig.BASE_STATES_DIRECTORY
import org.apache.hadoop.fs._
import scala.util.Try
import java.io._
import scala.Serializable
import com.google.common.io.ByteStreams

class HdfsState[T](val id: String, val hadoopConfiguration: SerializableHadoopConfiguration) extends Serializable {

  def set(value: T) = {
    val path = new Path(BASE_STATES_DIRECTORY, id)
    val fs = HdfsHelper.getFileSystemForPath(path, hadoopConfiguration.get())
    val os = fs.create(path)
    write(os, value)
  }

  def get(): Option[T] = Try {
    val path = new Path(BASE_STATES_DIRECTORY, id)
    val fs = HdfsHelper.getFileSystemForPath(path, hadoopConfiguration.get())
    val is = fs.open(path)
    read(is)
  }.toOption

  def getOrElse(default: T): T = get.getOrElse(default)

  def clear() = {
    val path = new Path(BASE_STATES_DIRECTORY, id)
    val fs = HdfsHelper.getFileSystemForPath(path, hadoopConfiguration.get())
    fs.delete(path, true)
  }

  private def write(os: FSDataOutputStream, value: T) = {
    val bytes = serialize(value)
    os.write(bytes)
    flush(os)
  }

  private def read(is: FSDataInputStream): T = {
    val bytes = ByteStreams.toByteArray(is)
    deserialize(bytes)
  }

  private def serialize(o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  private def deserialize(bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  private def flush(os: FSDataOutputStream) {
    hadoopFlushMethod.foreach { _.invoke(os) }
    // Useful for local file system where hflush/sync does not work (HADOOP-7844)
    os.getWrappedStream.flush()
  }
  private lazy val hadoopFlushMethod = {
    // Use reflection to get the right flush operation
    val cls = classOf[FSDataOutputStream]
    Try(cls.getMethod("hflush")).orElse(Try(cls.getMethod("sync"))).toOption
  }


}
