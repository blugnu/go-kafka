package confluent

func (p *Producer) Close() {
	p.funcs.Close()
}
