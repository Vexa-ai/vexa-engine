Business Requirements:

userflows


1.User share access to content to another user
2. User share access to enitity to another user
    - if enitity is global accross users that will mean that the accessing party will have access to contents that the providing party did not have, we dont want that
    - if entltly is local to the providing party that means the accessing party will have duplicated entities if they have interacted with this entitiy directly or got aceesss from another user, we dont want that

3. User want to be able to have automatic sharing of both contents and entities accross departments


please consider the following solution and provide feedback.








#TODO:


# Entity Access Control Design

## Problem Statement

Our system manages content (like meetings, documents) and entities (tags, speakers) with complex access control requirements:

1. **Dual Access Nature**
   - Users can access content directly through user-content relationships
   - Users can also access content through entities they have access to
   - Both paths must be considered when determining final access level

2. **Entity Types and Visibility**
   - Global entities (e.g., common speakers) are visible through content access
   - Local entities (e.g., personal tags) require explicit user access
   - Same content can have mix of global and local entities

3. **Access Level Inheritance**
   - Content has multiple access levels (REMOVED/SEARCH/TRANSCRIPT/OWNER)
   - Entities have simpler access levels (read/write)
   - Entity access must map to appropriate content access level
   - Highest access level from any path should be used

4. **Chat-Specific Requirements**
   - Need to resolve content IDs from multiple sources (direct IDs, entity IDs)
   - Must maintain consistent access control across chat context
   - Entity visibility must be preserved in chat responses
   - Performance critical for chat operations

5. **Performance Considerations**
   - Multiple access paths require efficient query optimization
   - Need to minimize database roundtrips
   - Must handle large sets of content and entities
   - Access checks happen frequently in chat context

6. **Design Constraints**
   - Maintain separation between content resolution and access control
   - Keep single source of truth for access decisions
   - Support future extension of access patterns
   - Enable clear debugging and auditing

## Core Concepts

1. **Entity Types**
   - Global entities (system-wide, like common speakers)
   - Local entities (user-specific, like personal tags)

2. **Access Paths**
   - Content access: user -> UserContent -> Content
   - Entity access: user -> UserEntityAccess -> Entity -> Content

## Data Model

```python
# psql_models.py
class Entity(Base):
    __tablename__ = 'entities'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(EntityType)
    is_global = Column(Boolean, default=False)
    owner_id = Column(UUID, ForeignKey('users.id'))
    
    # Relationships
    user_access = relationship('UserEntityAccess', back_populates='entity')
    contents = relationship('Content', secondary=content_entity_association)

class UserEntityAccess(Base):
    __tablename__ = 'user_entity_access'
    user_id = Column(UUID, ForeignKey('users.id'), primary_key=True)
    entity_id = Column(Integer, ForeignKey('entities.id'), primary_key=True)
    access_level = Column(String)  # read/write
    
    # Relationships
    entity = relationship('Entity', back_populates='user_access')
    user = relationship('User')
```

## Access Resolution Layer

```python
# access_resolver.py
class AccessResolver:
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def get_content_access_level(
        self,
        user_id: UUID,
        content_id: UUID,
        include_entity_access: bool = True
    ) -> AccessLevel:
        """Single source of truth for content access level"""
        # Direct content access
        direct_access = select(UserContent.access_level).where(and_(
            UserContent.user_id == user_id,
            UserContent.content_id == content_id
        ))
        
        if not include_entity_access:
            result = await self.session.scalar(direct_access)
            return AccessLevel(result) if result else AccessLevel.REMOVED
        
        # Entity-based access
        entity_access = select(
            case(
                (UserEntityAccess.access_level == 'write', AccessLevel.TRANSCRIPT),
                else_=AccessLevel.SEARCH
            ).label('access_level')
        ).select_from(content_entity_association).join(Entity).join(UserEntityAccess).where(
            and_(
                content_entity_association.c.content_id == content_id,
                UserEntityAccess.user_id == user_id,
                Entity.is_global.is_(False)
            )
        )
        
        # Combine access levels
        stmt = select(
            func.max(
                case(
                    (column('access_level') == AccessLevel.OWNER, 3),
                    (column('access_level') == AccessLevel.TRANSCRIPT, 2),
                    (column('access_level') == AccessLevel.SEARCH, 1),
                    else_=0
                )
            )
        ).select_from(union(direct_access, entity_access).subquery())
        
        result = await self.session.scalar(stmt)
        return AccessLevel(result) if result else AccessLevel.REMOVED

    async def get_visible_entities(
        self,
        user_id: UUID,
        content_id: Optional[UUID] = None,
        entity_ids: Optional[List[int]] = None
    ) -> List[Entity]:
        """Single source of truth for entity visibility"""
        conditions = [
            or_(
                # Global entities through content access
                and_(
                    Entity.is_global.is_(True),
                    Entity.id.in_(
                        select(content_entity_association.c.entity_id)
                        .join(UserContent, content_entity_association.c.content_id == UserContent.content_id)
                        .where(and_(
                            UserContent.user_id == user_id,
                            UserContent.access_level != AccessLevel.REMOVED
                        ))
                    )
                ),
                # Local entities through direct access
                and_(
                    Entity.is_global.is_(False),
                    Entity.id.in_(
                        select(UserEntityAccess.entity_id)
                        .where(UserEntityAccess.user_id == user_id)
                    )
                )
            )
        ]
        
        if content_id:
            conditions.append(
                Entity.id.in_(
                    select(content_entity_association.c.entity_id)
                    .where(content_entity_association.c.content_id == content_id)
                )
            )
            
        if entity_ids:
            conditions.append(Entity.id.in_(entity_ids))
            
        stmt = select(Entity).distinct().where(and_(*conditions))
        result = await self.session.execute(stmt)
        return result.scalars().unique().all()
    
    async def get_accessible_content_ids(
        self,
        user_id: UUID,
        content_id: Optional[UUID] = None,
        entity_id: Optional[int] = None,
        content_ids: Optional[List[UUID]] = None,
        entity_ids: Optional[List[int]] = None,
        min_access_level: AccessLevel = AccessLevel.SEARCH
    ) -> List[UUID]:
        """Get content IDs accessible to user through any path"""
        # Get base content IDs from resolver
        base_content_ids = await resolve_content_ids(
            self.session,
            content_id=content_id,
            entity_id=entity_id,
            content_ids=content_ids,
            entity_ids=entity_ids
        )
        
        if not base_content_ids:
            return []
            
        # Direct content access
        direct_access = select(UserContent.content_id).where(and_(
            UserContent.user_id == user_id,
            UserContent.content_id.in_(base_content_ids),
            UserContent.access_level >= min_access_level
        ))
        
        # Entity-based access
        entity_access = select(content_entity_association.c.content_id).join(
            Entity
        ).join(UserEntityAccess).where(and_(
            content_entity_association.c.content_id.in_(base_content_ids),
            UserEntityAccess.user_id == user_id,
            or_(
                Entity.is_global.is_(True),
                and_(
                    Entity.is_global.is_(False),
                    case(
                        (UserEntityAccess.access_level == 'write', AccessLevel.TRANSCRIPT),
                        else_=AccessLevel.SEARCH
                    ) >= min_access_level
                )
            )
        ))
        
        # Combine and deduplicate
        stmt = union(direct_access, entity_access)
        result = await self.session.execute(stmt)
        return list({row[0] for row in result})
```

## Service Layer Implementation

```python
# chat.py
class ChatRouter:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.access_resolver = AccessResolver(session)
    
    async def get_chat_content(
        self,
        user_id: UUID,
        content_id: Optional[UUID] = None,
        entity_id: Optional[int] = None,
        content_ids: Optional[List[UUID]] = None,
        entity_ids: Optional[List[int]] = None
    ) -> List[Content]:
        """Get chat content with proper access checks"""
        # Get accessible content IDs
        content_ids = await self.access_resolver.get_accessible_content_ids(
            user_id=user_id,
            content_id=content_id,
            entity_id=entity_id,
            content_ids=content_ids,
            entity_ids=entity_ids,
            min_access_level=AccessLevel.SEARCH
        )
        
        if not content_ids:
            return []
            
        # Fetch content with visible entities
        stmt = select(Content).where(Content.id.in_(content_ids))
        result = await self.session.execute(stmt)
        contents = result.scalars().unique().all()
        
        # Enrich with visible entities
        for content in contents:
            content.visible_entities = await self.access_resolver.get_visible_entities(
                user_id=user_id,
                content_id=content.id
            )
            
        return contents

# entity_manager.py
class EntityManager:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.access_resolver = AccessResolver(session)
    
    async def get_entities(
        self,
        user_id: UUID,
        content_id: Optional[UUID] = None,
        entity_ids: Optional[List[int]] = None
    ) -> List[Entity]:
        """Get visible entities with access resolution"""
        return await self.access_resolver.get_visible_entities(
            user_id=user_id,
            content_id=content_id,
            entity_ids=entity_ids
        )
```

## Key Benefits

1. **Unified Access Resolution**
   - Single source of truth for all access checks
   - Handles both direct and entity-based access
   - Supports chat-specific requirements

2. **Integration with Content Resolver**
   - Seamless integration with existing content resolution
   - Maintains separation of concerns
   - Efficient content ID resolution

3. **Flexible Access Control**
   - Support for minimum access levels
   - Optional entity access inclusion
   - Granular control over visibility

4. **Performance Optimized**
   - Efficient query construction
   - Minimal database roundtrips
   - Proper use of SQLAlchemy features

## Implementation Notes

1. **Access Resolution Flow**
   - Content resolver handles ID resolution
   - Access resolver enforces permissions
   - Services handle business logic

2. **Chat Integration**
   - Chat router uses access resolver directly
   - Supports both content and entity-based access
   - Maintains existing chat functionality

3. **Extension Points**
   - Easy to add new access patterns
   - Simple to modify access rules
   - Clear upgrade path for new features
