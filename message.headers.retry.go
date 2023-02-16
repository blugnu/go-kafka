package kafka

import "strconv"

func (msg *Message) IsRetryAttempt() (isRetry bool, attempt int, num int) {
	if s, ok := msg.Headers["blugnu/kafka:retry-attempt"]; ok {
		attempt, _ = strconv.Atoi(string(s))
	}
	if s, ok := msg.Headers["blugnu/kafka:retry-num"]; ok {
		num, _ = strconv.Atoi(string(s))
	}
	isRetry = attempt != 0 || num != 0
	return
}

func (msg *Message) setRetryHeaders(attempt, num int) {
	if attempt == 0 {
		attempt = 1
	}
	msg.SetHeader("blugnu/kafka:retry-attempt", strconv.Itoa(attempt))
	msg.SetHeader("blugnu/kafka:retry-num", strconv.Itoa(num))
}
