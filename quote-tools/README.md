# полная отчистка топика
./quote-kafka/build/quote-kafka-linux-amd64 purge --broker 95.217.61.39:9094 --tls \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

# пересоздание топика истории в kafka
./quote-kafka/build/quote-kafka-linux-amd64 recreate --broker 95.217.61.39:9094 --tls \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

# Пересоздать всё в clickhouse
./quote-ch/build/quote-ch-linux-amd64 recreate --host 95.217.61.39 --port 8443 \
  --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
  --kafka-user admin --kafka-password pass

# заполнение топика истории
./quote-gen/build/quote-gen-linux-amd64 --gen --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --broker 95.217.61.39:9094 --tls --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --symbol EURUSD \
    --topic quotes_history

# Очистка диапазона по символу и таймфрейму
./quote-ch/build/quote-ch-linux-amd64 clean --host 95.217.61.39 --port 8443 \
  --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
  --from "2025-02-15 20:30:00" --to "2025-02-15 21:00:00" \
  --symbol EURUSD

# заполнение топика истории с пересечением диапазона
./quote-gen/build/quote-gen-linux-amd64 --gen --from "2026-02-15 20:30:00" --to "2026-02-15 22:00:00" \
    --broker 95.217.61.39:9094 --tls --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --symbol EURUSD \
    --topic quotes_history

# генерация тиков по enter
./quote-gen/build/quote-gen-linux-amd64 --broker 95.217.61.39:9094 --tls --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --symbol EURUSD

