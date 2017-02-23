package main

import "github.com/shenwei356/pmap"
import "sync"

type Job struct {
	input string
	ochan chan int
}

type ChannelMap struct {
	words    *pmap.ParallelMap
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
	cm.words = pmap.NewParallelMap()
	cm.words.SetUpdateValueFunc(func(oldValue interface{}, newValue interface{}) interface{} {
		return oldValue.(int) + newValue.(int)
	})
	cm.killchan = make(chan int)
	cm.asc = make(chan Job, ASK_BUFFER_SIZE)
	cm.awc = make(chan string, ADD_BUFFER_SIZE)
	cm.rdc = make(chan reducepack)
	return cm
}

func (cm ChannelMap) Listen() {
	var wg sync.WaitGroup
	for {
		select {
		case _ = <-cm.killchan:
			return
		case msg := <-cm.awc:
			wg.Add(1)
			go func() {
				defer wg.Done()
				cm.add_word_impl(msg)
			}()
		case msg := <-cm.rdc:
			wg.Wait()
			word, count := cm.reduce_impl(msg.functor, msg.accum_str, msg.accum_int)
			msg.retchan <- reduceret{word, count}
		case msg := <-cm.asc:
			go cm.get_count_impl(msg.input, msg.ochan)
		}
	}
}

func (cm ChannelMap) Stop() {
	cm.killchan <- 1
}

func (cm ChannelMap) reduce_impl(functor ReduceFunc, accum_str string, accum_int int) (string, int) {
	for k, v := range cm.words.Map {
		accum_str, accum_int = functor(accum_str, accum_int, k.(string), v.(int))
	}
	return accum_str, accum_int
}

func (cm ChannelMap) Reduce(functor ReduceFunc, accum_str string, accum_int int) (string, int) {
	retchan := make(chan reduceret)
	cm.rdc <- reducepack{functor, accum_str, accum_int, retchan}
	a := <-retchan
	return a.word, a.count
}

func (cm ChannelMap) add_word_impl(word string) {
	_, ok := cm.words.Get(word)
	if ok {
		cm.words.Update(word, 1)
	} else {
		cm.words.Set(word, 1)
	}
}

func (cm ChannelMap) AddWord(word string) {
	cm.awc <- word
}

func (cm ChannelMap) get_count_impl(word string, retchan chan int) {
	count, ok := cm.words.Get(word)
	if ok {
		retchan <- count.(int)
	} else {
		retchan <- 0
	}
}

func (cm ChannelMap) GetCount(word string) int {
	rchan := make(chan int)
	cm.asc <- Job{word, rchan}
	return <-rchan
}
