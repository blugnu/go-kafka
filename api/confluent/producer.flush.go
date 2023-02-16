package confluent

func (p *Producer) Flush(timeout int) int {
	return p.funcs.Flush(timeout)
}
