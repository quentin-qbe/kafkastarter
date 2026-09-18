package com.qbe.kafkastarter.mapper;

import com.qbe.avro.Price;
import com.qbe.kafkastarter.dto.PriceDto;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface PriceMapper {

    Price toAvro(PriceDto dto);

    PriceDto fromAvro(Price event);
}
