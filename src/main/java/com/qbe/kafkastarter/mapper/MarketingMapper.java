package com.qbe.kafkastarter.mapper;

import com.qbe.avro.Marketing;
import com.qbe.kafkastarter.dto.MarketingDto;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface MarketingMapper {

    Marketing toAvro(MarketingDto dto);

    MarketingDto fromAvro(Marketing event);
}
