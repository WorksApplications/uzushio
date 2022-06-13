package org.sample.corpus

import com.worksap.nlp.sudachi.{DictionaryFactory, Tokenizer}

object Sudachi {
  def parseSplitMode(mode: String): Tokenizer.SplitMode = {
    // Parse sudachi SplitMode from a string.
    mode.capitalize match {
      case "A" => Tokenizer.SplitMode.A
      case "B" => Tokenizer.SplitMode.B
      case _   => Tokenizer.SplitMode.C
    }
  }

  def setupSudachiTokenizer(): Tokenizer = {
    // create sudachi Tokenizer instance.
    // system_core.dict must be in cwd.
    // TODO: load config file
    new DictionaryFactory().create().create()
  }
}
