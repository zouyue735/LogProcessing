package indv.zy.base

import scala.reflect._
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

class ImplicitList[+A: TypeTag : ClassTag](val _0: Seq[A]) extends Dynamic {

  lazy val m: Mirror = runtimeMirror(getClass.getClassLoader)
  lazy val _0r: universe.InstanceMirror = m.reflect(_0)
  lazy val _0t: universe.Type = typeOf[Seq[A]]

  def selectDynamic[B: TypeTag : ClassTag](fieldName: String): ImplicitList[B] = {
    if (fieldName.startsWith("_")) {
      val field = _0t.decl(TermName(fieldName.tail)).asTerm
      ImplicitList[B](_0r.reflectField(field).get.asInstanceOf[B])
    } else {
      val field = typeOf[A].decl(TermName(fieldName)).asTerm
      new ImplicitList[B](_0.map(m.reflect(_).reflectField(field).get.asInstanceOf[B]))
    }
  }

  def applyDynamic[B: TypeTag : ClassTag](methodName: String)(params: Any*): ImplicitList[B] = {
    if (methodName.startsWith("_")) {
      val method = _0t.decl(TermName(methodName.tail)).asMethod
      ImplicitList[B](_0r.reflectMethod(method).apply(params: _*).asInstanceOf[B])
    } else {
      val method = typeOf[A].decl(TermName(methodName)).asMethod
      ImplicitList[B](_0.map(m.reflect(_).reflectMethod(method).apply(params: _*).asInstanceOf[B]))
    }
  }
}

class Foo2(val bar1: Int) {
  def bar2(n: Int): Int = n + bar1
}

object ImplicitList {

  def apply[A: TypeTag : ClassTag]: ImplicitList[A] = new ImplicitList[A](Seq())

  def apply[A: TypeTag : ClassTag](list: Seq[A]): ImplicitList[A] = new ImplicitList[A](list)

  def apply[A: TypeTag : ClassTag](single: A): ImplicitList[A] = new ImplicitList[A](Seq(single))

  def main(args: Array[String]): Unit = {
    val foo1 = new Foo2(1)
    val foo2 = new Foo2(2)

    val il: ImplicitList[Foo2] = ImplicitList[Foo2](List())
    val il2 = il.+:(foo1).+:(foo2)
    println(il2.bar1)
    println(il2.bar2(1))
    println(il2._map(_.bar1 + 111).bar1)
  }
}