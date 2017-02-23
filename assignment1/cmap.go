package main

type Job struct {
	input string
	ochan chan int
}

type ChannelMap struct {
	words    map[string]int
	killchan chan int
	asc      chan Job
	awc      chan string
	rdc      chan reducepack
}

type reduceret struct {
	word  string
	count int
}

type reducepack struct {
	functor   ReduceFunc
	accum_str string
	accum_int int
	retchan   chan reduceret
}

func NewChannelMap() *ChannelMap {
	cm := new(ChannelMap)
	cm.words = make(map[string]int)
	cm.killchan = make(chan int)
	cm.asc = make(chan Job, ASK_BUFFER_SIZE)
	cm.awc = make(chan string, ADD_BUFFER_SIZE)
	cm.rdc = make(chan reducepack)
	return cm
}

func (cm ChannelMap) Listen() {
	for {
		select {
		case _ = <-cm.killchan:
			return
		case msg := <-cm.awc:
			cm.add_word_impl(msg)
		case msg := <-cm.rdc:
			words := make(map[string]int)
			for k, v := range cm.words {
				words[k] = v
			}
			go cm.reduce_impl(msg.functor, msg.accum_str, msg.accum_int, msg.retchan, words)
		case msg := <-cm.asc:
			msg.ochan <- cm.get_count_impl(msg.input)
		}
	}
}

func (cm ChannelMap) Stop() {
	cm.killchan <- 1
}

func (cm ChannelMap) reduce_impl(functor ReduceFunc, accum_str string, accum_int int, retchan chan reduceret, words map[string]int) {
	for k, v := range words {
		accum_str, accum_int = functor(accum_str, accum_int, k, v)
	}
	retchan <- reduceret{accum_str, accum_int}
}

func (cm ChannelMap) Reduce(functor ReduceFunc, accum_str string, accum_int int) (string, int) {
	retchan := make(chan reduceret)
	cm.rdc <- reducepack{functor, accum_str, accum_int, retchan}
	a := <-retchan
	return a.word, a.count
}

func (cm ChannelMap) add_word_impl(word string) {
	count, ok := cm.words[word]
	if ok {
		cm.words[word] = count + 1
	} else {
		cm.words[word] = 1
	}
}

func (cm ChannelMap) AddWord(word string) {
	cm.awc <- word
}

func (cm ChannelMap) get_count_impl(word string) int {
	count, ok := cm.words[word]
	if ok {
		return count
	}
	return 0
}

func (cm ChannelMap) GetCount(word string) int {
	rchan := make(chan int)
	cm.asc <- Job{word, rchan}
	return <-rchan
}
